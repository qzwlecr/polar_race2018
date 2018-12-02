// Copyright [2018] Alibaba Cloud All rights reserved
#ifdef _POSIX_C_SOURCE
#undef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 201012
#endif

#include "engine_race.h"
#include "consts/consts.h"
#include "task/task.h"
#include "format/log.h"
#include "index/index.h"
#include "syscalls/sys.h"
#include "perf/perf.h"
#include "bucket/bucket.h"
#include "bucket/bucket_link_list.h"
#include <thread>


extern "C" {
#include <malloc.h>
#include <unistd.h>
#include "signames.h"
#include <signal.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/sem.h>
}

#define FAILED_TEXT "[" Q_FMT_APPLY(Q_COLOR_RED) "FAIL" Q_FMT_APPLY(Q_COLOR_RESET) "] (**SIGNAL**)"

void signal_handler(int sig) {
    fprintf(stderr, "%s received signal %s: %s\n", FAILED_TEXT, signal_names[sig], strsignal(sig));
    exit(-1);
}

void signal_dump(int sig, siginfo_t *siginfo, void *vuctx) {
    fprintf(stderr, "%s received signal %s: %s\n", FAILED_TEXT, signal_names[sig], strsignal(sig));
    psiginfo(siginfo, FAILED_TEXT "Signal info dump ");
    exit(-1);
}

namespace polar_race {

    RetCode Engine::Open(const std::string &name, Engine **eptr) {
        return EngineRace::Open(name, eptr);
    }

    Engine::~Engine() {
    }

    const int UDS_NUM = CONCURRENT_QUERY * UDS_CONGEST_AMPLIFIER;

    std::string VALUES_PATH;
    std::string INDECIES_PATH;
    std::string META_PATH;
    std::string OFFSET_TABLE_PATH;

    std::string recvaddres[HANDLER_THREADS];
    struct sockaddr_un rsaddr[HANDLER_THREADS];
    TimingProfile handtps[HANDLER_THREADS] = {{0}};
    std::atomic_bool reqfds_occupy[UDS_NUM];
    MailBox requestfds[UDS_NUM];
    Accumulator requestId(0);
    bool running = true;
    volatile int lockfd = -1;
    std::thread *selfclsr = nullptr;
    Accumulator newaff(0);

    std::string ItoS(int i) {
        char tmp[40] = {0};
        sprintf(tmp, "%d", i);
        return std::string(tmp);
    }

/*
 * Complete the functions below to implement you own engine
 */


// 1. Open engine
    RetCode EngineRace::Open(const std::string &name, Engine **eptr) {
        *eptr = NULL;
        EngineRace *engine_race = new EngineRace(name);
        VALUES_PATH = name + VALUES_PATH_SUFFIX;
        INDECIES_PATH = name + INDECIES_PATH_SUFFIX;
        META_PATH = name + META_PATH_SUFFIX;
        OFFSET_TABLE_PATH = name + OFFSET_TABLE_PATH_SUFFIX;
        if (EXEC_MODE_BENCHMARK) {
            qLogSuccfmt("Startup: Bencher %s", name.c_str());
            if (access(name.c_str(), F_OK)) {
                mkdir(name.c_str(), 0755);
            }
            BenchMain(name);
        }
        qLogSuccfmt("Startup: EngineName %s", name.c_str());
        qLogInfofmt("Startup: Checking %s existence", VALUES_PATH.c_str());
        if (access(VALUES_PATH.c_str(), R_OK | W_OK)) {
            qLogInfofmt("Startup: Not exist: CREATING %s", VALUES_PATH.c_str());
            if (access(name.c_str(), F_OK)) {
                mkdir(name.c_str(), 0755);
            }
            creat(VALUES_PATH.c_str(), 0666);
            lockfd = open(VALUES_PATH.c_str(), 0);
            int lockv = flock(lockfd, LOCK_EX);
            if (lockv != 0) {
                qLogFailfmt("Startup: Acquiring file lock failed: %s", strerror(errno));
                abort();
            }
        } else {
            qLogSuccfmt("Startup: Acquiring Lock of %s", VALUES_PATH.c_str());
            lockfd = open(VALUES_PATH.c_str(), 0);
            int lockv = flock(lockfd, LOCK_EX);
            if (lockv != 0) {
                qLogFailfmt("Startup: Acquiring file lock failed: %s", strerror(errno));
                abort();
            }
        }
        for (int i = 0; i < BUCKET_BUFFER_LENGTH; i++) {
            BucketLinkLists[i] = new BucketLinkList(i);
        }
        if (access(META_PATH.c_str(), R_OK | W_OK)) {
            MetaFd = open(META_PATH.c_str(), O_RDWR | O_CREAT, 0666);
            NextIndex = 0;
        } else {
            MetaFd = open(META_PATH.c_str(), O_RDWR | O_CREAT, 0666);
            uint64_t data_size = 0;
            if (read(MetaFd, &data_size, sizeof(uint64_t)) == -1) {
                qLogFailfmt("Startup: Read file size failed: %s", strerror(errno));
                abort();
            }
            NextIndex = data_size;
            BucketLinkList::unpersist(MetaFd);
        }
        ValuesFd = open(VALUES_PATH.c_str(), O_RDWR | O_APPEND | O_SYNC | O_CREAT | O_DIRECT, 0666);

        if (posix_fallocate(ValuesFd, 0, FILE_SIZE)) {
            qLogFailfmt("Startup: Fallocate file failed: %s", strerror(errno));
            abort();
        }

        int sem = semget(IPC_PRIVATE, 1, 0666 | IPC_CREAT);
        if (sem == -1) {
            qLogFailfmt("Startup: Acquiring semophore failed: %s", strerror(errno));
            abort();
        }
        if (semctl(sem, 0, SETVAL, 1) == -1) {
            qLogFailfmt("Startup: Set semophore failed: %s", strerror(errno));
            abort();
        }
        qLogInfo("Startup: resetting Global Variables");
        running = true;
        requestId = 0;
        newaff = 0;
        for (int i = 0; i < HANDLER_THREADS; i++) {
            handtps[i] = {0};
        }
        if (SELFCLOSER_ENABLED) {
            qLogSuccfmt("Startup: Starting SelfCloser, sanity time %d", SANITY_EXEC_TIME);
            selfclsr = new std::thread(SelfCloser, SANITY_EXEC_TIME, &running);
            qLogInfo("Startup: SelfCloser started.");
        }
        qLogSuccfmt("StartupConfigurator: %d Handlers..", HANDLER_THREADS);
        for (int i = 0; i < HANDLER_THREADS; i++) {
            recvaddres[i] = std::string(REQ_ADDR_PREFIX) + ItoS(i);
            rsaddr[i] = mksockaddr_un(recvaddres[i]);
        }
        qLogInfofmt("StartupConfigurator: %d UDSs..", UDS_NUM);
        for (int i = 0; i < UDS_NUM; i++) {
            std::string sndaddr = std::string(RESP_ADDR_PREFIX) + ItoS(i);
            requestfds[i] = MailBox(sndaddr);
            if (requestfds[i].desc == -1) {
                qLogFailfmt("Startup: UDS %d open failed: %s", i, strerror(errno));
                abort();
            }
            reqfds_occupy[i] = false;
        }
        qLogInfo("Startup: FORK !");
        if (fork()) {
            // parent
            qLogSucc("Startup: FORK completed.");
            qLogSucc("Startup: HeartBeat thread.");
            std::thread hbthread(HeartBeater, HB_ADDR, &running);
            hbthread.detach();
            // qLogInfo("Startup: wait ReqHandler startup complete.");
            struct sembuf sem_buf{
                    .sem_num = 0,
                    .sem_op = 0
            };
            semop(sem, &sem_buf, 1);
            qLogSucc("Startup: Everything OK.");
        } else {
            // child
            qLogInfo("RequestHandler: FORK completed.");
            qLogInfo("RequestHandler: Setup termination detector.");
            if (!SIGNAL_FULL_DUMP) {
                signal(SIGABRT, signal_handler);
                signal(SIGFPE, signal_handler);
                signal(SIGINT, signal_handler);
                signal(SIGSEGV, signal_handler);
                signal(SIGTERM, signal_handler);
            } else {
                struct sigaction repact = {0};
                repact.sa_sigaction = signal_dump;
                repact.sa_flags = SA_SIGINFO;
                sigemptyset(&(repact.sa_mask));
                int sigv = 0;
                sigv = sigaction(SIGABRT, &repact, NULL);
                if (sigv == -1) {
                    qLogWarnfmt("RequestHandler: prepare signal dump for signal SIGABRT failed: %s", strerror(errno));
                }
                sigv = sigaction(SIGFPE, &repact, NULL);
                if (sigv == -1) {
                    qLogWarnfmt("RequestHandler: prepare signal dump for signal SIGFPE failed: %s", strerror(errno));
                }
                sigv = sigaction(SIGINT, &repact, NULL);
                if (sigv == -1) {
                    qLogWarnfmt("RequestHandler: prepare signal dump for signal SIGINT failed: %s", strerror(errno));
                }
                sigv = sigaction(SIGSEGV, &repact, NULL);
                if (sigv == -1) {
                    qLogWarnfmt("RequestHandler: prepare signal dump for signal SIGSEGV failed: %s", strerror(errno));
                }
                sigv = sigaction(SIGTERM, &repact, NULL);
                if (sigv == -1) {
                    qLogWarnfmt("RequestHandler: prepare signal dump for signal SIGTERM failed: %s", strerror(errno));
                }
            }
            for (int i = 0; i < BUCKET_NUMBER; i++) {
                Buckets[i] = new Bucket(i);
            }
            size_t pagesize = (size_t) getpagesize();
            for (int i = 0; i < BUCKET_BACKUP_NUMBER; i++) {
                BackupBuffer[i] = (char *) memalign(pagesize, BUCKET_BUFFER_LENGTH);
            }
            BucketThreadPool = new thread_pool(WRITE_CONCURRENCY);
            qLogInfofmt("RequestHandlerConfigurator: %d Handler threads..", HANDLER_THREADS);
            for (int i = 0; i < HANDLER_THREADS; i++) {
                qLogInfofmt("RequestHander: Starting Handler thread %d", i);
                std::thread handthrd(RequestProcessor, recvaddres[i], &(handtps[i]), uint8_t(i));
                handthrd.detach();
            }
            qLogSucc("RequestHandler: starting HeartBeat Detection thread..");
            GlobalIndexStore = new IndexStore();
            qLogSuccfmt("StartupConfigurator: Unpersisting Core Index from %s", INDECIES_PATH.c_str());
            if (!access(INDECIES_PATH.c_str(), R_OK | W_OK)) {
                qLogInfo("Startup: Unpersisting..");
                int fd = open(INDECIES_PATH.c_str(), 0);
                GlobalIndexStore->unpersist(fd);
                close(fd);
            }
            std::thread hbdtrd(HeartBeatChecker, HB_ADDR);
            hbdtrd.detach();
            struct sembuf sem_buf{
                    .sem_num = 0,
                    .sem_op = -1
            };
            semop(sem, &sem_buf, 1);
            qLogSucc("RequestHandler: everything OK, will now go to indefinite sleep!!");
            while (true) {
                select(1, NULL, NULL, NULL, NULL);
            }
            qLogFail("RequestHandler: finished waiting from select(). exiting//");
            exit(1);
        }
        *eptr = engine_race;
        return kSucc;
    }

// 2. Close engine
    EngineRace::~EngineRace() {
        qLogSucc("Engine:: Destructing..");
        running = false;
        qLogSucc("Engine:: Closing UDSs..");
        for (int i = 0; i < UDS_NUM; i++) {
            if (requestfds[i].close()) {
                qLogInfofmt("Closing: socket %d close failed: %s", i, strerror(errno));
            }
        }
        if (SELFCLOSER_ENABLED) {
            qLogSucc("Engine:: Waiting SelfCloser exit..");
            selfclsr->join();
        }
    }

// 3. Write a key-value pair into engine
    RetCode EngineRace::Write(const PolarString &key, const PolarString &value) {
        qLogDebugfmt("Engine::Write: K %hu => V %hu",
                     *reinterpret_cast<const uint16_t *>(key.data()),
                     *reinterpret_cast<const uint16_t *>(value.data()));
        int curraff = CONCURRENT_QUERY;
        int gav = sys_getaff(0, &curraff);
        if (gav == -1) {
            qLogWarnfmt("Engine::Write get affinity failed: %s", strerror(errno));
        }
        qLogDebugfmt("Engine::Write: affinity %d", curraff);
        if (curraff == CONCURRENT_QUERY) {
            int newaffinity = (int) (newaff.fetch_add(1) % CONCURRENT_QUERY);
            sys_setaff(0, newaffinity);
            qLogInfofmt("Engine::Write new affinity %d", newaffinity);
            curraff = newaffinity;
        }
        RequestResponse rr = {0};
        memcpy(rr.key, key.data(), KEY_SIZE);
        memcpy(rr.value, value.data(), VAL_SIZE);
        rr.type = RequestType::TYPE_WR;
        ssize_t sv = requestfds[curraff].sendOne(reinterpret_cast<char *>(&rr), sizeof(RequestResponse),
                                                 &(rsaddr[curraff / 2]));
        if (sv == -1) {
            return kIOError;
        }
        struct sockaddr_un useless;
        ssize_t rv = requestfds[curraff].getOne(reinterpret_cast<char *>(&rr), sizeof(RequestResponse), &useless);
        if (rv == -1) {
            return kIOError;
        }
        if (rr.type != RequestType::TYPE_OK) {
            return kNotFound;
        }
        return kSucc;
    }

// 4. Read value of a key
    RetCode EngineRace::Read(const PolarString &key, std::string *value) {
        RequestResponse rr = {0};
        memcpy(rr.key, key.data(), KEY_SIZE);
        rr.type = RequestType::TYPE_RD;
        // Acquire an Mailbox
        uint64_t reqIdx = 0;
        bool fk = false;
        do {
            reqIdx = requestId.fetch_add(1);
            fk = false;
        } while (!reqfds_occupy[reqIdx % UDS_NUM].compare_exchange_weak(fk, true));
        qLogDebugfmt("Engine::Read Using Socket %lu K %s", reqIdx, KVArrayDump(rr.key, 8).c_str());
        // then OK, we do writing work
        ssize_t sv = requestfds[reqIdx % UDS_NUM].sendOne(
                reinterpret_cast<char *>(&rr), sizeof(RequestResponse), &(rsaddr[reqIdx % HANDLER_THREADS]));
        if (sv == -1) {
            reqfds_occupy[reqIdx % UDS_NUM] = false;
            return kIOError;
        }
        struct sockaddr_un useless;
        ssize_t rv = requestfds[reqIdx % UDS_NUM].getOne(
                reinterpret_cast<char *>(&rr), sizeof(RequestResponse), &useless);
        reqfds_occupy[reqIdx % UDS_NUM] = false;
        if (rv != sizeof(RequestResponse)) {
            qLogWarnfmt("Engine::Read failed or incomplete: %s(%ld)", strerror(errno), rv);
            return kIOError;
        }
        if (rr.type != RequestType::TYPE_OK) {
            return kNotFound;
        }
        qLogDebugfmt("Engine::Read Complete K %s V %s", KVArrayDump(rr.key, 8).c_str(),
                     KVArrayDump(rr.value, 8).c_str());
        *value = std::string(rr.value, VAL_SIZE);
        return kSucc;
    }

/*
 * NOTICE: Implement 'Range' in quarter-final,
 *         you can skip it in preliminary.
 */
// 5. Applies the given Vistor::Visit function to the result
// of every key-value pair in the key range [first, last),
// in order
// lower=="" is treated as a key before all keys in the database.
// upper=="" is treated as a key after all keys in the database.
// Therefore the following call will traverse the entire database:
//   Range("", "", visitor)
    RetCode EngineRace::Range(const PolarString &lower, const PolarString &upper,
                              Visitor &visitor) {
       /* TODO: what should we do?
        * according to discussion, this part will work like:
        * (OOO: Once and Only Once)
        * 1. Tell the backend to close, and re-acquire the file lock..
        * 2. Read the offset hash table into memory OOO, lookup for range borders, and free it.
        * 3. Read the offset table into memory OOO
        * 4. Record the loaded cache blocks and corresponding file offset range
        * 5. On read each offset, check whether it's already loaded
        * 6. If not, wait until it's loaded.
        * 7. If access to one cache reached it's possible maximum, remove it from memory.
        * -----------------------------------------------------
        * In order to ensure the OOO property, we may need some global variables
        * to control concurrent requests. What's more, we need a cache object to control
        * the overall access to file contents. This cache object also need to be created OOO.
        */
        return kNotSupported;
    }

}  // namespace polar_race
