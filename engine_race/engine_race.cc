// Copyright [2018] Alibaba Cloud All rights reserved
#ifdef _POSIX_C_SOURCE
#undef _POSIX_C_SOURCE
#endif
#define _POSIX_C_SOURCE 201012
#include "engine_race.h"
#include "consts/consts.h"
#include "task/task.h"
#include "format/log.h"
#include "flusher/flusher.h"
#include "index/index.h"
#include "commu/commu.h"
#include <cstdio>
#include <cstdlib>
#include <thread>


extern "C"{
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

    std::string recvaddres[HANDLER_THREADS];
    struct sockaddr_un rsaddr[HANDLER_THREADS];
    TimingProfile handtps[HANDLER_THREADS] = {{0}};
    MailBox requestfds[UDS_NUM];
    std::atomic_bool reqfds_occupy[UDS_NUM];
    Accumulator requestId(0);
    Accumulator completeRd(0);
    Flusher *flusher;
    bool running = true;
    volatile int lockfd = -1;
    std::thread* selfclsr = nullptr;
    int operationfds[UDS_NUM] = {0};

    // =========== FOR RW Mode

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
        if(EXEC_MODE == MODE_MPROC_RAND_WR){
            qLogSucc("Startup: Engine mode set to MPORC_RAND_WR");
            return OpenRW(name, eptr);
        }
        completeRd = 0;
        *eptr = NULL;
        EngineRace *engine_race = new EngineRace(name);
        VALUES_PATH = name + VALUES_PATH_SUFFIX;
        INDECIES_PATH = name + INDECIES_PATH_SUFFIX;
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
            struct stat valfstat = {0};
            int sv = stat(VALUES_PATH.c_str(), &valfstat);
            if (sv != 0) {
                qLogFailfmt("Startup: Values file exist, but unable to get its size: %s", strerror(errno));
                abort();
            }
            qLogSuccfmt("Startup: Set file size to %lu", valfstat.st_size);
            WrittenIndex = valfstat.st_size;
            NextIndex = valfstat.st_size;
        }
        qLogSucc("Startup: opening operation fds");
        for(int i = 0; i < UDS_NUM; i++){
            operationfds[i] = open(VALUES_PATH.c_str(), O_NOATIME | O_RDONLY);
            if(operationfds[i] == -1){
                qLogFailfmt("Startup: unable to open operfd[%d]: %s",
                        i, strerror(errno));
                // without the operation fd, this program simply won't work.
                abort();
            }
        }
        int sem = semget(IPC_PRIVATE, 1, 0666|IPC_CREAT);
        if(sem == -1){
            qLogFailfmt("Startup: Acquiring semophore failed: %s", strerror(errno));
            abort();
        }
        if(semctl(sem, 0, SETVAL, 1) == -1) {
            qLogFailfmt("Startup: Set semophore failed: %s", strerror(errno));
            abort();
        }
        qLogInfo("Startup: resetting Global Variables");
        running = true;
        for(int i = 0; i < HANDLER_THREADS; i++){
            handtps[i] = {0};
        }
        if(SELFCLOSER_ENABLED){
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
            flusher = new Flusher();
            qLogInfofmt("RequestHandlerConfigurator: %d Handler threads..", HANDLER_THREADS);
            for (int i = 0; i < HANDLER_THREADS; i++) {
                qLogInfofmt("RequestHander: Starting Handler thread %d", i);
                std::thread handthrd(RequestProcessor, recvaddres[i], &(handtps[i]));
                handthrd.detach();
            }
            qLogSucc("RequestHandler: starting Disk Operation thread..");
            flusher->flush_begin();
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
        if(EXEC_MODE == MODE_MPROC_RAND_WR){
            DtorRW(*this);
            if(SELFCLOSER_ENABLED){
                qLogSucc("Engine:: Waiting SelfCloser exit..");
                selfclsr->join();
            }
            return;
        }
        running = false;
        qLogSucc("Engine:: Closing UDSs ..");
        for (int i = 0; i < UDS_NUM; i++) {
            if (requestfds[i].close()) {
                qLogInfofmt("Closing: socket %d close failed: %s", i, strerror(errno));
            }
            if (close(operationfds[i])) {
                qLogWarnfmt("Closing: opfd %d close failed: %s", i, strerror(errno));
            }
        }
        if(SELFCLOSER_ENABLED){
            qLogSucc("Engine:: Waiting SelfCloser exit..");
            selfclsr->join();
        }
    }

// 3. Write a key-value pair into engine
    RetCode EngineRace::Write(const PolarString &key, const PolarString &value) {
        qLogDebugfmt("Engine::Write: K %hu => V %hu",
                     *reinterpret_cast<const uint16_t *>(key.data()),
                     *reinterpret_cast<const uint16_t *>(value.data()));
        if(EXEC_MODE == MODE_MPROC_RAND_WR){
            return WriteRW(key, value);
        }
        WriteRequest wr = {0};
        WriteResponse wresp = {0};
        wr.type = RequestType::TYPE_WR;
        memcpy(wr.key, key.data(), KEY_SIZE);
        memcpy(wr.value, value.data(), VAL_SIZE);
        // Acquire an Mailbox
        uint64_t reqIdx = 0;
        bool fk = false;
        do {
            reqIdx = requestId.fetch_add(1);
            fk = false;
        } while (reqfds_occupy[reqIdx % UDS_NUM].compare_exchange_strong(fk, true) == false);
        unsigned accessIdx = reqIdx % UDS_NUM;
        // then OK, we do writing work
        ssize_t sv = requestfds[accessIdx].sendOne(
                reinterpret_cast<char *>(&wr), sizeof(WriteRequest), &(rsaddr[accessIdx % HANDLER_THREADS]));
        if (sv == -1) {
            reqfds_occupy[accessIdx] = false;
            qLogFailfmt("Engine::Write cannot send: %s", strerror(errno));
            return kIOError;
        }
        struct sockaddr_un useless;
        ssize_t rv = requestfds[accessIdx].getOne(
                reinterpret_cast<char *>(&wresp), sizeof(WriteResponse), &useless);
        reqfds_occupy[accessIdx] = false;
        if (rv == -1) {
            qLogFailfmt("Engine::Write cannot get commit message: %s", strerror(errno));
            return kIOError;
        }
        if (wresp.type != RequestType::TYPE_OK) {
            return kNotFound;
        }
        return kSucc;
    }

// 4. Read value of a key
    RetCode EngineRace::Read(const PolarString &key, std::string *value) {
        if(EXEC_MODE == MODE_MPROC_RAND_WR){
            return ReadRW(key, value);
        }
        qChar('I');
        ReadRequest rr = {0};
        memcpy(rr.key, key.data(), KEY_SIZE);
        rr.type = RequestType::TYPE_RD;
        ReadResponseAny rrany;
        memset(&rrany, 0, sizeof(ReadResponseAny));
        // Acquire an Mailbox
        uint64_t reqIdx = 0;
        bool fk = false;
        do {
            reqIdx = requestId.fetch_add(1);
            fk = false;
        } while (reqfds_occupy[reqIdx % UDS_NUM].compare_exchange_strong(fk, true) == false);
        /* if(reqIdx % 1000000 == 0){ */
        /*     qLogSuccfmt("Engine::Read total request number hit %lu", reqIdx); */
        /* } */
        qChar('A');
        unsigned accessIdx = reqIdx % UDS_NUM;
        // then OK, we do writing work
        ssize_t sv = requestfds[accessIdx].sendOne(
                reinterpret_cast<char *>(&rr), sizeof(ReadRequest), &(rsaddr[accessIdx % HANDLER_THREADS]));
        if (sv != sizeof(ReadRequest)) {
            qLogWarnfmt("Engine::Read send failed or incomplete: %s(%ld)", strerror(errno), sv);
            reqfds_occupy[accessIdx] = false;
            return kIOError;
        }
        qChar('S');
        struct sockaddr_un useless;
        ssize_t rv = requestfds[accessIdx].getOne(
                reinterpret_cast<char *>(&rrany), sizeof(ReadResponseAny), &useless);
        if (rv != sizeof(ReadResponse) && rv != sizeof(ReadResponseFull)) {
            qLogWarnfmt("Engine::Read recv failed or incomplete: %s(%ld)", strerror(errno), rv);
            reqfds_occupy[accessIdx] = false;
            return kIOError;
        }
        qChar('R');
        if(rv == sizeof(ReadResponse)){
            qChar('!');
            // we still need the occupy flag to prevent mixing of operfd
            ReadResponse *resp = reinterpret_cast<ReadResponse*>(&rrany);
            if (resp->type != RequestType::TYPE_OK) {
                reqfds_occupy[accessIdx] = false;
                qLogDebugfmt("Engine::Read K %s not found on disk.", KVArrayDump(key.data(), 8).c_str());
                return kNotFound;
            }
            qLogDebugfmt("Engine::Read Complete with foffset %lu", resp->foffset);
            // we need to read thing off by ourselves.
            // copy some logic from sequential mode
            uint64_t rdoffset = resp->foffset;
            int lv = lseek(operationfds[accessIdx], rdoffset, SEEK_SET);
            if(lv == -1){
                qLogFailfmt("Engine: lseek failed to move to %lu: %s", rdoffset,
                        strerror(errno));
                reqfds_occupy[accessIdx] = false;
                return kIOError;
            }
            char valarr[VAL_SIZE] = {0};
            ssize_t wv = read(operationfds[accessIdx], valarr, VAL_SIZE);
            if(wv != VAL_SIZE){
                qLogFailfmt("Engine: failed or incomplete read: %s(%ld)",
                        strerror(errno), wv);
                reqfds_occupy[accessIdx] = false;
                return kIOError;
            }
            reqfds_occupy[accessIdx] = false;
            qLogDebugfmt("Engine::Read Complete K %s V %s", KVArrayDump(key.data(), 8).c_str(),
                         KVArrayDump(valarr, 8).c_str());
            *value = std::string(valarr, VAL_SIZE);
            /* completeRd.fetch_add(1); */
            return kSucc;
        } else {
            qChar('D');
            reqfds_occupy[accessIdx] = false;
            ReadResponseFull* respf = reinterpret_cast<ReadResponseFull*>(&rrany);
            if (respf->type != RequestType::TYPE_OK) {
                qLogDebugfmt("Engine::Read K %s not found on InternalBuffer.", KVArrayDump(key.data(), 8).c_str());
                return kNotFound;
            }
            qLogDebugfmt("Engine::Read Complete K %s V %s", KVArrayDump(key.data(), 8).c_str(),
                         KVArrayDump(respf->value, 8).c_str());
            *value = std::string(respf->value, VAL_SIZE);
            qChar('E');
            /* completeRd.fetch_add(1); */
            return kSucc;
        }
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
        return kNotSupported;
    }

    // =================================================================================
    // RW Funcs
    // =================================================================================
    
    
    RetCode OpenRW(const std::string &name, Engine **eptr){
        *eptr = NULL;
        EngineRace *engine_race = new EngineRace(name);
        VALUES_PATH = name + VALUES_PATH_SUFFIX;
        INDECIES_PATH = name + INDECIES_PATH_SUFFIX;
        qLogSuccfmt("StartupRW: EngineName %s", name.c_str());
        qLogInfofmt("StartupRW: Checking %s existence", VALUES_PATH.c_str());
        if (access(VALUES_PATH.c_str(), R_OK | W_OK)) {
            qLogInfofmt("StartupRW: Not exist: CREATING %s", VALUES_PATH.c_str());
            if (access(name.c_str(), F_OK)) {
                mkdir(name.c_str(), 0755);
            }
            creat(VALUES_PATH.c_str(), 0666);
            lockfd = open(VALUES_PATH.c_str(), 0);
            int lockv = flock(lockfd, LOCK_EX);
            if (lockv != 0) {
                qLogFailfmt("StartupRW: Acquiring file lock failed: %s", strerror(errno));
                abort();
            }
        } else {
            qLogSuccfmt("StartupRW: Acquiring Lock of %s", VALUES_PATH.c_str());
            lockfd = open(VALUES_PATH.c_str(), 0);
            int lockv = flock(lockfd, LOCK_EX);
            if (lockv != 0) {
                qLogFailfmt("StartupRW: Acquiring file lock failed: %s", strerror(errno));
                abort();
            }
            struct stat valfstat = {0};
            int sv = stat(VALUES_PATH.c_str(), &valfstat);
            if (sv != 0) {
                qLogFailfmt("StartupRW: Values file exist, but unable to get its size: %s", strerror(errno));
                abort();
            }
            qLogSuccfmt("StartupRW: Set file size to %lu", valfstat.st_size);
            // RW mode don't need WrittenIdx
            /* WrittenIndex = valfstat.st_size; */
            NextIndex = valfstat.st_size;
        }
        qLogSucc("StartupRW: opening operation fds");
        for(int i = 0; i < UDS_NUM; i++){
            operationfds[i] = open(VALUES_PATH.c_str(), O_DSYNC | O_RDWR);
            if(operationfds[i] == -1){
                qLogFailfmt("StartupRW: unable to open operfd[%d]: %s",
                        i, strerror(errno));
                // without the operation fd, this program simply won't work.
                abort();
            }
        }
        int sem = semget(IPC_PRIVATE, 1, 0666|IPC_CREAT);
        if(sem == -1){
            qLogFailfmt("StartupRW: Acquiring semophore failed: %s", strerror(errno));
            abort();
        }
        if(semctl(sem, 0, SETVAL, 1) == -1) {
            qLogFailfmt("StartupRW: Set semophore failed: %s", strerror(errno));
            abort();
        }
        qLogInfo("StartupRW: resetting Global Variables");
        running = true;
        for(int i = 0; i < HANDLER_THREADS; i++){
            handtps[i] = {0};
        }
        if(SELFCLOSER_ENABLED){
            qLogSuccfmt("StartupRW: Starting SelfCloser, sanity time %d", SANITY_EXEC_TIME);
            selfclsr = new std::thread(SelfCloser, SANITY_EXEC_TIME, &running);
            qLogInfo("StartupRW: SelfCloser started.");
        }
        qLogSuccfmt("StartupConfiguratorRW: %d Handlers..", HANDLER_THREADS);
        for (int i = 0; i < HANDLER_THREADS; i++) {
            recvaddres[i] = std::string(REQ_ADDR_PREFIX) + ItoS(i);
            rsaddr[i] = mksockaddr_un(recvaddres[i]);
        }
        qLogSuccfmt("StartupConfiguratorRW: %d UDSs..", UDS_NUM);
        for (int i = 0; i < UDS_NUM; i++) {
            std::string sndaddr = std::string(RESP_ADDR_PREFIX) + ItoS(i);
            requestfds[i] = MailBox(sndaddr);
            if (requestfds[i].desc == -1) {
                qLogFailfmt("StartupRW: UDS %d open failed: %s", i, strerror(errno));
                abort();
            }
            reqfds_occupy[i] = false;
        }
        qLogSucc("StartupRW: FORK !");
        if (fork()) {
            // parent
            qLogSucc("StartupRW: fork completed.");
            qLogSucc("StartupRW: Starting HeartBeat thread.");
            std::thread hbthread(HeartBeater_rw, HB_ADDR, &running);
            hbthread.detach();
            qLogSucc("StartupRW: wait RequestHandlerRW startup complete.");
            struct sembuf sem_buf{
                    .sem_num = 0,
                    .sem_op = 0
            };
            semop(sem, &sem_buf, 1);
            qLogSucc("StartupRW: Everything OK.");
        } else {
            // child
            qLogSucc("RequestHandlerRW: fork completed.");
            qLogSucc("RequestHandlerRW: Setup abnormal termination detector.");
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
                    qLogWarnfmt("RequestHandlerRW: prepare signal dump for signal SIGABRT failed: %s", strerror(errno));
                }
                sigv = sigaction(SIGFPE, &repact, NULL);
                if (sigv == -1) {
                    qLogWarnfmt("RequestHandlerRW: prepare signal dump for signal SIGFPE failed: %s", strerror(errno));
                }
                sigv = sigaction(SIGINT, &repact, NULL);
                if (sigv == -1) {
                    qLogWarnfmt("RequestHandlerRW: prepare signal dump for signal SIGINT failed: %s", strerror(errno));
                }
                sigv = sigaction(SIGSEGV, &repact, NULL);
                if (sigv == -1) {
                    qLogWarnfmt("RequestHandlerRW: prepare signal dump for signal SIGSEGV failed: %s", strerror(errno));
                }
                sigv = sigaction(SIGTERM, &repact, NULL);
                if (sigv == -1) {
                    qLogWarnfmt("RequestHandlerRW: prepare signal dump for signal SIGTERM failed: %s", strerror(errno));
                }
            }
            flusher = new Flusher();
            qLogSuccfmt("RequestHandlerConfiguratorRW: %d Handler threads..", HANDLER_THREADS);
            for (int i = 0; i < HANDLER_THREADS; i++) {
                qLogInfofmt("RequestHanderRW: Starting Handler thread %d", i);
                std::thread handthrd(RequestProcessor_rw, recvaddres[i], &(handtps[i]));
                handthrd.detach();
            }
            // For RW, we dont need separate disk flushing thread
            /* qLogSucc("RequestHandlerRW: starting Disk Operation thread.."); */
            /* flusher->flush_begin(); */
            GlobalIndexStore = new IndexStore();
            qLogSuccfmt("RequestHandlerRW: Unpersisting Core Index from %s", INDECIES_PATH.c_str());
            if (!access(INDECIES_PATH.c_str(), R_OK | W_OK)) {
                qLogInfo("RequestHandlerRW: Unpersisting..");
                int fd = open(INDECIES_PATH.c_str(), 0);
                GlobalIndexStore->unpersist(fd);
                close(fd);
            }
            qLogSucc("RequestHandlerRW: starting HeartBeat Detection thread..");
            std::thread hbdtrd(HeartBeatChecker_rw, HB_ADDR);
            hbdtrd.detach();
            struct sembuf sem_buf{
                    .sem_num = 0,
                    .sem_op = -1
            };
            semop(sem, &sem_buf, 1);
            qLogSucc("RequestHandlerRW: Everything OK, will now go to indefinite sleep!!");
            while (true) {
                select(1, NULL, NULL, NULL, NULL);
            }
            qLogFail("RequestHandlerRW: finished waiting from select(). exiting//");
            exit(1);
        }
        *eptr = engine_race;
        return kSucc;
    }
    RetCode WriteRW(const PolarString& key, const PolarString& value){
        RequestResponseRW rr = {0};
        memcpy(rr.key, key.data(), KEY_SIZE);
        rr.type = RequestTypeRW::TYPERW_PUT;
        // Get NextIndex
        uint64_t wroffset = NextIndex.fetch_add(VAL_SIZE);
        rr.foffset = wroffset;
        // Acquire an Mailbox
        uint64_t reqIdx = 0;
        bool fk = false;
        do {
            reqIdx = requestId.fetch_add(1);
            fk = false;
        } while (reqfds_occupy[reqIdx % UDS_NUM].compare_exchange_strong(fk, true) == false);
        // then OK, we do writing work
        ssize_t sv = requestfds[reqIdx % UDS_NUM].sendOne(
                reinterpret_cast<char *>(&rr), sizeof(RequestResponseRW), &(rsaddr[reqIdx % HANDLER_THREADS]));
        if (sv == -1) {
            reqfds_occupy[reqIdx % UDS_NUM] = false;
            qLogFailfmt("Engine(RW): Cannot send put request: %s", strerror(errno));
            return kIOError;
        }
        struct sockaddr_un useless;
        ssize_t rv = requestfds[reqIdx % UDS_NUM].getOne(
                reinterpret_cast<char *>(&rr), sizeof(RequestResponseRW), &useless);
        if (rv != sizeof(RequestResponseRW)) {
            qLogFailfmt("Engine(RW): Failed or incomplete recv: %s(%ld)",
                    strerror(errno), rv);
            reqfds_occupy[reqIdx % UDS_NUM] = false;
            return kIOError;
        }
        if (rr.type != RequestType::TYPE_OK) {
            reqfds_occupy[reqIdx % UDS_NUM] = false;
            return kNotFound;
        }
        // initiate write process
        int opidx = reqIdx % UDS_NUM;
        int lv = lseek(operationfds[opidx], wroffset, SEEK_SET);
        if(lv == -1){
            qLogFailfmt("Engine(RW): lseek failed to move to %lu: %s", wroffset,
                    strerror(errno));
            reqfds_occupy[reqIdx % UDS_NUM] = false;
            return kIOError;
        }
        ssize_t wv = write(operationfds[opidx], value.data(), VAL_SIZE);
        if(wv != VAL_SIZE){
            qLogFailfmt("Engine(RW): failed or incomplete write: %s(%ld) <- %d",
                    strerror(errno), wv, operationfds[opidx]);
            reqfds_occupy[reqIdx % UDS_NUM] = false;
            return kIOError;
        }
        reqfds_occupy[reqIdx % UDS_NUM] = false;
        return kSucc;
    }
    RetCode ReadRW(const PolarString& key, std::string *value){
        RequestResponseRW rr = {0};
        memcpy(rr.key, key.data(), KEY_SIZE);
        rr.type = RequestTypeRW::TYPERW_GET;
        // Acquire an Mailbox
        uint64_t reqIdx = 0;
        bool fk = false;
        do {
            reqIdx = requestId.fetch_add(1);
            fk = false;
        } while (reqfds_occupy[reqIdx % UDS_NUM].compare_exchange_strong(fk, true) == false);
        // then OK, we do writing work
        ssize_t sv = requestfds[reqIdx % UDS_NUM].sendOne(
                reinterpret_cast<char *>(&rr), sizeof(RequestResponseRW), &(rsaddr[reqIdx % HANDLER_THREADS]));
        if (sv == -1) {
            reqfds_occupy[reqIdx % UDS_NUM] = false;
            qLogFailfmt("Engine(RW): Cannot send get request: %s", strerror(errno));
            return kIOError;
        }
        struct sockaddr_un useless;
        ssize_t rv = requestfds[reqIdx % UDS_NUM].getOne(
                reinterpret_cast<char *>(&rr), sizeof(RequestResponseRW), &useless);
        if (rv != sizeof(RequestResponseRW)) {
            qLogFailfmt("Engine(RW): Failed or incomplete recv: %s(%ld)",
                    strerror(errno), rv);
            reqfds_occupy[reqIdx % UDS_NUM] = false;
            return kIOError;
        }
        if (rr.type != RequestType::TYPE_OK) {
            reqfds_occupy[reqIdx % UDS_NUM] = false;
            return kNotFound;
        }
        // initiate read process
        int opidx = reqIdx % UDS_NUM;
        uint64_t rdoffset = rr.foffset;
        int lv = lseek(operationfds[opidx], rdoffset, SEEK_SET);
        if(lv == -1){
            qLogFailfmt("Engine(RW): lseek failed to move to %lu: %s", rdoffset,
                    strerror(errno));
            reqfds_occupy[reqIdx % UDS_NUM] = false;
            return kIOError;
        }
        char valarr[VAL_SIZE] = {0};
        ssize_t wv = read(operationfds[opidx], valarr, VAL_SIZE);
        if(wv != VAL_SIZE){
            qLogFailfmt("Engine(RW): failed or incomplete read: %s(%ld)",
                    strerror(errno), wv);
            reqfds_occupy[reqIdx % UDS_NUM] = false;
            return kIOError;
        }
        *value = std::string(valarr, VAL_SIZE);
        reqfds_occupy[reqIdx % UDS_NUM] = false;
        return kSucc;
    }
    void DtorRW(EngineRace& engine){
        running = false;
        qLogSucc("EngineRW:: Closing UDSs and operfds..");
        for (int i = 0; i < UDS_NUM; i++) {
            if (requestfds[i].close()) {
                qLogWarnfmt("Closing: socket %d close failed: %s", i, strerror(errno));
            }
            if (close(operationfds[i])) {
                qLogWarnfmt("Closing: opfd %d close failed: %s", i, strerror(errno));
            }
        }
    }

}  // namespace polar_race
