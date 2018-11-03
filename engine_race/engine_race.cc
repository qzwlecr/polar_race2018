// Copyright [2018] Alibaba Cloud All rights reserved
#include "engine_race.h"
#include <unistd.h>
#include "consts/consts.h"
#include "task/task.h"
#include "format/log.h"
#include <thread>
#include "flusher/flusher.h"
#include "index/index.h"
#include <cstdio>
#include <stdlib.h>
#include <malloc.h>

extern "C"{
    #include "signames.h"
#include "signal.h"
#include <sys/stat.h>
#include <sys/file.h>
}

using namespace std;

#define FAILED_TEXT "[  " Q_FMT_APPLY(Q_COLOR_RED) "FAIL" Q_FMT_APPLY(Q_COLOR_RESET) "  ]"

void signal_handler(int sig){
    fprintf(stderr, "%s received signal %s: %s\n", FAILED_TEXT, signal_names[sig], strsignal(sig));
    exit(-1);
}

namespace polar_race {

    RetCode Engine::Open(const std::string &name, Engine **eptr) {
        return EngineRace::Open(name, eptr);
    }

    Engine::~Engine() {
    }

    const int UDS_NUM = CONCURRENT_QUERY * UDS_CONGEST_AMPLIFIER;

    string recvaddres[HANDLER_THREADS];
    struct sockaddr_un rsaddr[HANDLER_THREADS];
    MailBox requestfds[UDS_NUM];
    atomic_bool reqfds_occupy[UDS_NUM];
    Accumulator requestId(0);
    bool start_ok = false;
    bool running = true;
    volatile int lockfd = -1;

    Flusher flusher;

    string ItoS(int i) {
        char tmp[40] = {0};
        sprintf(tmp, "%d", i);
        return string(tmp);
    }

/*
 * Complete the functions below to implement you own engine
 */


// 1. Open engine
    RetCode EngineRace::Open(const std::string &name, Engine **eptr) {
        *eptr = NULL;
        EngineRace *engine_race = new EngineRace(name);
        qLogInfofmt("Startup: EngineName %s", name.c_str());
        qLogInfofmt("Startup: Checking %s existence", VALUES_PATH.c_str());
        if(access(VALUES_PATH.c_str(), R_OK | W_OK)){
            qLogInfofmt("Startup: Not exist: CREATING %s", VALUES_PATH.c_str());
            creat(VALUES_PATH.c_str(), 0666);
            lockfd = open(VALUES_PATH.c_str(), 0);
            int lockv = flock(lockfd, LOCK_EX);
            if(lockv != 0){
                qLogFailfmt("Startup: Acquiring file lock failed: %s", strerror(errno));
                abort();
            }
        } else {
            qLogInfofmt("Startup: Acquiring Lock of %s", VALUES_PATH.c_str());
            lockfd = open(VALUES_PATH.c_str(), 0);
            int lockv = flock(lockfd, LOCK_EX);
            if(lockv != 0){
                qLogFailfmt("Startup: Acquiring file lock failed: %s", strerror(errno));
                abort();
            }
            struct stat valfstat = {0};
            int sv = stat(VALUES_PATH.c_str(), &valfstat);
            if(sv != 0){
                qLogFailfmt("Startup: Values file exist, but unable to get its size: %s", strerror(errno));
                abort();
            }
            qLogInfofmt("Startup: Set file size to %lu", valfstat.st_size);
            WrittenIndex = valfstat.st_size;
            NextIndex = valfstat.st_size;
        }
        qLogInfofmt("StartupConfigurator: %d Handlers..", HANDLER_THREADS);
        for (int i = 0; i < HANDLER_THREADS; i++) {
            recvaddres[i] = string(REQ_ADDR_PREFIX) + ItoS(i);
            rsaddr[i] = mksockaddr_un(recvaddres[i]);
        }
        qLogInfofmt("StartupConfigurator: %d UDSs..", UDS_NUM);
        for (int i = 0; i < UDS_NUM; i++) {
            string sndaddr = string(RESP_ADDR_PREFIX) + ItoS(i);
            requestfds[i] = MailBox(sndaddr);
            if (requestfds[i].desc == -1) {
                qLogFailfmt("Startup: UDS %d open failed: %s", i, strerror(errno));
                abort();
            }
            reqfds_occupy[i] = false;
        }
        qLogInfofmt("StartupConfigurator: Unpersisting Core Index from %s", INDECIES_PATH.c_str());
        if (!access(INDECIES_PATH.c_str(), R_OK | W_OK)) {
            qLogInfo("Startup: Unpersisting..");
            int fd = open(INDECIES_PATH.c_str(), 0);
            global_index_store.unpersist(fd);
            close(fd);
        }
        InternalBuffer = (char*)memalign(4096, INTERNAL_BUFFER_LENGTH);
        qLogInfo("Startup: FORK !");
        if (fork()) {
            // parent
            qLogInfo("Startup: FORK completed.");
            qLogInfo("Startup: wait ReqHandler startup complete.");
            sleep(2);
            qLogInfo("Startup: HeartBeat thread.");
            thread hbthread(HeartBeater, HB_ADDR, &running);
            hbthread.detach();
            qLogInfo("Startup: Everything OK.");
        } else {
            // child
            qLogInfo("RequestHandler: FORK completed.");
            qLogInfo("RequestHandler: Setup termination detector.");
            signal(SIGABRT, signal_handler);
            signal(SIGFPE, signal_handler);
            signal(SIGINT, signal_handler);
            signal(SIGSEGV, signal_handler);
            signal(SIGTERM, signal_handler);
            qLogInfofmt("RequestHandlerConfigurator: %d Handler threads..", HANDLER_THREADS);
            for (int i = 0; i < HANDLER_THREADS; i++) {
                qLogInfofmt("RequestHander: Starting Handler thread %d", i);
                thread handthrd(RequestProcessor, recvaddres[i]);
                handthrd.detach();
            }
            qLogInfo("RequestHandler: starting Disk Operation thread..");
            flusher.flush_begin();
            qLogInfo("RequestHandler: starting HeartBeat Detection thread..");
            thread hbdtrd(HeartBeatChecker, HB_ADDR);
            hbdtrd.detach();
            start_ok = true;
            qLogInfo("RequestHandler: everything OK, will now go to indefinite sleep!!");
            select(1, NULL, NULL, NULL, NULL);
            qLogFail("RequestHandler: finished waiting from select(). exiting//");
            exit(1);
        }
        *eptr = engine_race;
        return kSucc;
    }

// 2. Close engine
    EngineRace::~EngineRace() {
        running = false;
        qLogInfo("Closing: closing sockets..");
        for (int i = 0; i < UDS_NUM; i++) {
            if(requestfds[i].close()){
                qLogInfofmt("Closing: socket %d close failed: %s", i, strerror(errno));
            }
        }
    }

// 3. Write a key-value pair into engine
    RetCode EngineRace::Write(const PolarString &key, const PolarString &value) {
        qLogDebugfmt("Engine::Write: K %hu => V %hu",
                *reinterpret_cast<const uint16_t*>(key.data()), *reinterpret_cast<const uint16_t*>(value.data()));
        RequestResponse rr = {0};
        memcpy(rr.key, key.data(), KEY_SIZE);
        memcpy(rr.value, value.data(), VAL_SIZE);
        rr.type = RequestType::TYPE_WR;
        // Acquire an Mailbox
        uint64_t reqIdx = 0;
        bool fk = false;
        do {
            reqIdx = requestId.fetch_add(1);
            fk = false;
        } while (reqfds_occupy[reqIdx % UDS_NUM].compare_exchange_strong(fk, true) == false);
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
        } while (reqfds_occupy[reqIdx % UDS_NUM].compare_exchange_strong(fk, true) == false);
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
        if (rv != sizeof(RequestResponse)){
            qLogWarnfmt("Engine::Read failed or incomplete: %s(%ld)", strerror(errno), rv);
            return kIOError;
        }
        if (rr.type != RequestType::TYPE_OK) {
            return kNotFound;
        }
        qLogDebugfmt("Engine::Read Complete K %s V %s", KVArrayDump(rr.key, 8).c_str(), KVArrayDump(rr.value, 8).c_str());
        *value = string(rr.value, VAL_SIZE);
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
        return kNotSupported;
    }

}  // namespace polar_race
