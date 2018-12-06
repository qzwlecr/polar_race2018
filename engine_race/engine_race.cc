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
#include "rcache/rcache.h"
#include <thread>
#include <atomic>


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
    std::string KEY_TABLE_PATH;

    std::string recvaddres[HANDLER_THREADS];
    struct sockaddr_un rsaddr[HANDLER_THREADS];
    TimingProfile handtps[HANDLER_THREADS] = {{0}};
    std::atomic_bool reqfds_occupy[UDS_NUM];
    MailBox requestfds[UDS_NUM];
    bool running = true;
    std::atomic_bool is_forked;
    bool is_forked_real;
    std::atomic_bool is_initialized;
    bool is_initialized_real;

    int valuesfdr = 0;
    volatile int lockfd = -1;
    uint64_t indexer_size = 0;
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
        KEY_TABLE_PATH = name + KEY_TABLE_PATH_SUFFIX;
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
        int unlockv = flock(lockfd, LOCK_UN);
        if (unlockv != 0) {
            qLogFailfmt("Startup: Release file lock failed: %s", strerror(errno));
            abort();
        }
        for (int i = 0; i < BUCKET_NUMBER; i++) {
            BucketLinkLists[i] = new BucketLinkList(i);
        }
        if (access(META_PATH.c_str(), R_OK | W_OK)) {
            MetaFd = open(META_PATH.c_str(), O_RDWR | O_CREAT, 0666);
            NextIndex = 0;
            for (int i = 0; i < BUCKET_NUMBER; i++) {
                BucketLinkLists[i]->sizes.push_back(0);
                auto index = NextIndex.fetch_add(FIRST_BUCKET_LENGTH);
                BucketLinkLists[i]->links.push_back(index);
            }
        } else {
            MetaFd = open(META_PATH.c_str(), O_RDWR | O_CREAT, 0666);
            uint64_t data_size = 0;
            if (read(MetaFd, &data_size, sizeof(uint64_t)) == -1) {
                qLogFailfmt("Startup: Read file size failed: %s", strerror(errno));
                abort();
            }
            if (read(MetaFd, &indexer_size, sizeof(uint64_t)) == -1) {
                qLogFailfmt("Startup: Read indexer size failed: %s", strerror(errno));
                abort();
            }
            qLogDebugfmt("Startup: Read file size: %lu", data_size);
            qLogDebugfmt("Startup: Read indexer size: %lu", indexer_size);
            NextIndex = data_size;
            BucketLinkList::unpersist(MetaFd);
            for (int i = 0; i < BUCKET_NUMBER; i++) {
                BucketLinkLists[i]->sizes.push_back(0);
                auto index = NextIndex.fetch_add(OTHER_BUCKET_LENGTH);
                BucketLinkLists[i]->links.push_back(index);
            }
        }
        ValuesFd = open(VALUES_PATH.c_str(), O_RDWR | O_SYNC |  O_DIRECT, 0666);
        valuesfdr = open(VALUES_PATH.c_str(), O_NOATIME);

        if (posix_fallocate(ValuesFd, 0, FILE_SIZE)) {
            qLogFailfmt("Startup: Fallocate file failed: %s", strerror(errno));
            abort();
        }

        is_forked = false;
        is_forked_real = false;
        is_initialized = false;
        is_initialized_real = false;

        if (SELFCLOSER_ENABLED) {
            qLogSuccfmt("Startup: Starting SelfCloser, sanity time %d", SANITY_EXEC_TIME);
            selfclsr = new std::thread(SelfCloser, SANITY_EXEC_TIME, &running);
            qLogInfo("Startup: SelfCloser started.");
        }

        *eptr = engine_race;
        return kSucc;
    }

    void EngineRace::prepareSignalDump() {
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
        close(lockfd);
        lockfd = -1;
    }

// 3. Write a key-value pair into engine
    RetCode EngineRace::Write(const PolarString &key, const PolarString &value) {
        if (!is_forked_real) {
            bool forked = false;
            do {
                forked = false;
            } while (!is_forked.compare_exchange_weak(forked, true));
            if (!is_forked_real) {
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
                newaff = 0;
                for (int i = 0; i < HANDLER_THREADS; i++) {
                    handtps[i] = {0};
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
                    prepareSignalDump();
                    int lockv = flock(lockfd, LOCK_EX);
                    if(lockv == -1){
                        qLogFailfmt("RequstHandler: unable to acquire file lock: %s", strerror(errno));
                        abort();
                    }
                    for (int i = 0; i < BUCKET_NUMBER; i++) {
                        Buckets[i] = new Bucket(i);
                        Buckets[i]->head_index = BucketLinkLists[i]->links.back();
                        Buckets[i]->next_index = Buckets[i]->head_index;
                    }
                    size_t pagesize = (size_t) getpagesize();
                    for (int i = 0; i < BUCKET_BACKUP_NUMBER; i++) {
                        BackupBuffer[i] = (char *) memalign(pagesize, BUCKET_BUFFER_LENGTH);
                    }
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
            }
            is_forked_real = true;
            is_forked = false;

        }
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
            qLogFailfmt("Engine::Write failed to send wr request: MB[%d]:%s", requestfds[curraff].desc, strerror(errno));
            abort();
            return kIOError;
        }
        struct sockaddr_un useless;
        ssize_t rv = requestfds[curraff].getOne(reinterpret_cast<char *>(&rr), sizeof(RequestResponse), &useless);
        if (rv == -1) {
            qLogFailfmt("Engine::Write failed to recv wr response: MB[%d]:%s", requestfds[curraff].desc, strerror(errno));
            abort();
            return kIOError;
        }
        return kSucc;
    }

// 4. Read value of a key
    RetCode EngineRace::Read(const PolarString &key, std::string *value) {
        if (!is_initialized_real) {
            bool checker = false;
            do {
                checker = false;
            } while (!is_initialized.compare_exchange_weak(checker, true));
            if (!is_initialized_real) {
                GlobalIndexStore = new IndexStore();
                qLogSuccfmt("StartupConfigurator: Unpersisting Core Index from %s", INDECIES_PATH.c_str());
                if (!access(INDECIES_PATH.c_str(), R_OK | W_OK)) {
                    qLogInfo("Startup: Unpersisting..");
                    int fd = open(INDECIES_PATH.c_str(), 0);
                    GlobalIndexStore->unpersist(fd);
                    close(fd);
                }
            }
            is_initialized_real = true;
            is_initialized = false;
        }

        uint64_t k = *reinterpret_cast<uint64_t *>(const_cast<char * >(key.data()));
        char val[VAL_SIZE];
        uint64_t file_offset = 0;
        // look up in global index store
        if (!GlobalIndexStore->get(k, file_offset)) {
            // not found
            qLogInfofmt("EngineRace: Key %lu not found !", k);
            return kNotFound;
        } else {
            // that means we should read it from file
            // read things off it
//            StartTimer(&t);
            ssize_t rdv = pread(valuesfdr, val, VAL_SIZE, file_offset);
//            uint64_t rdtel = GetTimeElapsed(&t);
//            tp->rddsk.accumulate((rdtel / 1000));
//            tp->read_disk += rdtel;
            if (UNLIKELY(rdv != VAL_SIZE)) {
                qLogWarnfmt("EngineRace: read failed or incomplete: %s(%ld), treated as NOT FOUND.", strerror(errno),
                            rdv);
                return kNotFound;
            } else {
                qLogDebugfmt("EngineRace: Value read off disk: %lu %s", k, KVArrayDump(val, 2).c_str());
                *value = std::string(val, VAL_SIZE);
                return kSucc;
            }
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
    struct RangeStats {
        std::atomic_int concur_ranges;
        std::atomic_int globidx_completed;
        bool state_running;
        RangeCache* rcache;
        int metafd;
        int backfd;
        RangeStats():
        concur_ranges(0), state_running(false),
        globidx_completed(0),
        rcache(nullptr), metafd(-1), backfd(-1){};
        void load_globidx();
        void unload_globidx();
        off_t read_globidx(const char* key);
        void load_globofftab();
        void unload_globofftab();
        off_t read_globofftab(uint32_t elemidx);
        void load_globkeytab();
        void unload_globkeytab();
        const char* read_globkeytab(uint32_t elemidx);
    } rs;
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
        // wait for previous range epoch complete
        off_t loweroff, upperoff;
        uint32_t nextelem = 0;
        const char* rdkey = nullptr;
        char* rdval = (char*)malloc(VAL_SIZE);
        while(rs.state_running);
        int myid = rs.concur_ranges.fetch_add(1);
        if(myid == 0){
            // this is the first Ranger, it need to initialize many things..
            rs.metafd = open(META_PATH.c_str(), O_RDONLY);
            if(rs.metafd == -1){
                qLogFailfmt("FirstRanger: metafd open failed: %s", strerror(errno));
                abort();
            }
            rs.backfd = open(VALUES_PATH.c_str(), O_RDONLY);
            if(rs.backfd == -1){
                qLogFailfmt("FirstRanger: backfd open failed: %s", strerror(errno));
                abort();
            }
            qLogSuccfmt("FirstRanger: meta %s, values %s, cachesz %u",
            META_PATH_SUFFIX.c_str(), VALUES_PATH_SUFFIX.c_str(), RANGE_CACHE_SIZE);
            qLogSucc("FirstRanger: Creating RangeCache..");
            rs.rcache = new RangeCache(rs.metafd, rs.backfd, RANGE_CACHE_SIZE);
            qLogSucc("FirstRanger: Loading GlobalIndex");
            rs.load_globidx();
            if(lower.ToString() != ""){
                loweroff = rs.read_globidx(lower.data());
            }
            if(upper.ToString() != ""){
                upperoff = rs.read_globidx(upper.data());
            }
            rs.globidx_completed.fetch_add(1);
            while(rs.globidx_completed != 64);
            rs.unload_globidx();
            rs.load_globofftab();
            rs.load_globkeytab();
            if(lower.ToString() == ""){
                loweroff = rs.read_globofftab(0);
            }
            if(upper.ToString() == ""){
                upperoff = rs.read_globofftab(indexer_size - 1);
            }
            for(uint32_t i = 0; i < indexer_size; i++){
                if(rs.read_globofftab(i) == loweroff){
                    nextelem = i;
                    break;
                }
            }
            // differential part ends here..
        } else {
            if(myid == (CONCURRENT_QUERY - 1)){
                rs.state_running = true;
            }
            if(lower.ToString() != ""){
                loweroff = rs.read_globidx(lower.data());
            }
            if(upper.ToString() != ""){
                upperoff = rs.read_globidx(upper.data());
            }
            rs.globidx_completed.fetch_add(1);
            if(lower.ToString() == ""){
                loweroff = rs.read_globofftab(0);
            }
            if(upper.ToString() == ""){
                upperoff = rs.read_globofftab(indexer_size - 1);
            }
            for(uint32_t i = 0; i < indexer_size; i++){
                if(rs.read_globofftab(i) == loweroff){
                    nextelem = i;
                    break;
                }
            }
        }
        // start the reading work..
        while(true){
            off_t rdoffset = rs.read_globofftab(nextelem);
            rdkey = rs.read_globkeytab(nextelem);
            if(!rs.rcache->access(rdoffset, rdval)){
                qLogFailfmt("Ranger: rcache access failed: %lu", rdoffset);
                abort();
            }
            visitor.Visit(PolarString(rdkey, KEY_SIZE), PolarString(rdval, VAL_SIZE));
            if(rdoffset == upperoff) break;
            nextelem ++;
        }
        if(rs.concur_ranges.fetch_sub(1) == 1){
            // this is the last ranger
            qLogSucc("LastRanger: resetting globals");
            rs.globidx_completed = 0;
            qLogSucc("LastRanger: unloading data structures..");
            rs.unload_globofftab();
            rs.unload_globkeytab();
            qLogSucc("LastRanger: destructing RangeCache..");
            delete rs.rcache;
            qLogSucc("LastRanger: closing file descriptors..");
            close(rs.metafd);
            close(rs.backfd);
            qLogSucc("LastRanger: resetting globals pass 2");
            rs.rcache = nullptr;
            rs.metafd = -1;
            rs.backfd = -1;
            qLogSucc("LastRanger: marking epoch completion..");
            rs.state_running = false;
        }
        qLogSucc("Ranger: waiting exit sign..");
        while(rs.state_running);
        return kSucc;
    }

    void RangeStats::load_globidx(){
        if(GlobalIndexStore != nullptr){
            qLogWarn("RangeStatsLoader: GlobalIndexStore is not null while trying to load it.");
            qLogWarn("RangeStatsLoader: will treat as warning, but this usually indicates critical bugs.");
            return;
        }
        IndexStore* idst = new IndexStore();
        int idxsfd = open(INDECIES_PATH.c_str(), O_RDONLY);
        if(idxsfd == -1){
            qLogFailfmt("RangeStatsLoader: cannot open index path: %s", strerror(errno));
            abort();
        }
        qLogDebug("RangeStatsLoader: start index unpersisting..");
        idst->unpersist(idxsfd);
        close(idxsfd);
        GlobalIndexStore = idst;
        qLogSucc("RangeStatsLoader: index unpersist completed.");
    }

    void RangeStats::unload_globidx(){
        delete GlobalIndexStore;
        GlobalIndexStore = nullptr;
        qLogSucc("RangeStatsEliminator: GlobalIndexStore eliminated.");
    }
    
    void RangeStats::load_globofftab(){
        if(GlobalOffsetTable != nullptr){
            qLogWarn("RangeStatsLoader: GlobalOffsetTable is not null while trying to load it.");
            qLogWarn("RangeStatsLoader: will treat as warning, but this usually indicates critical bugs.");
            return;
        }
        int offtfd = open(OFFSET_TABLE_PATH.c_str(), O_RDONLY);
        if(offtfd == -1){
            qLogFailfmt("RangeStatsLoader: cannot open offset path: %s", strerror(errno));
            abort();
        }
        OffsetTable *offtab = new OffsetTable(offtfd);
        GlobalOffsetTable = offtab;
        close(offtfd);
        qLogSucc("RangeStatsLoader: offset table unpersisted.");
    }

    void RangeStats::unload_globofftab(){
        delete GlobalOffsetTable;
        GlobalOffsetTable = nullptr;
        qLogSucc("RangeStatsEliminator: GlobalOffsetTable eliminated.");
    }

    void RangeStats::load_globkeytab(){
        if(GlobalKeyTable != nullptr){
            qLogWarn("RangeStatsLoader: GlobalKeyTable is not null while trying to load it.");
            qLogWarn("RangeStatsLoader: will treat as warning, but this usually indicates critical bugs.");
            return;
        }
        int offtfd = open(KEY_TABLE_PATH.c_str(), O_RDONLY);
        if(offtfd == -1){
            qLogFailfmt("RangeStatsLoader: cannot open key path: %s", strerror(errno));
            abort();
        }
        KeyTable* ktab = new KeyTable(offtfd);
        GlobalKeyTable = ktab;
        close(offtfd);
        qLogSucc("RangeStatsLoader: key table unpersisted.");
    }

    void RangeStats::unload_globkeytab(){
        delete GlobalKeyTable;
        GlobalKeyTable = nullptr;
        qLogSucc("RangeStatsEliminator: GlobalKeyTable eliminated.");
    }

    off_t RangeStats::read_globidx(const char* key){
        while(GlobalIndexStore == nullptr);
        uint64_t pos = 0;
        bool exist = GlobalIndexStore->get(*(const uint64_t*)(key), pos);
        if(!exist){
            qLogWarn("GlobalIndexReader: are you trying to get some not-exist key in range??");
        }
        return pos;
    }

    off_t RangeStats::read_globofftab(uint32_t elemidx){
        while(GlobalOffsetTable == nullptr);
        if(elemidx >= indexer_size){
            qLogWarnfmt("GlobalOffsetReader: overflow detected, trying read %u, only has %u", elemidx, 0);
            return 0;
        }
        return REAL_OFFSET(GlobalOffsetTable->data[elemidx]);
    }

    const char* RangeStats::read_globkeytab(uint32_t elemidx){
        while(GlobalKeyTable == nullptr);
        if(elemidx >= indexer_size){
            qLogWarnfmt("GlobalKeyReader: overflow detected, trying read %u, only has %u", elemidx, 0);
            return 0;
        }
        return (char*)&(GlobalKeyTable->data[elemidx]);
    }
}  // namespace polar_race
