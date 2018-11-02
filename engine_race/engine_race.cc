// Copyright [2018] Alibaba Cloud All rights reserved
#include "engine_race.h"
#include <unistd.h>
#include "consts/consts.h"
#include "task/task.h"
#include "format/log.h"
#include <thread>
#include "flusher/flusher.h"
#include "index/index.h"

using namespace std;

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
        qLogInfofmt("Startup: Pre-Creating %s to prevent Hander gg..", VALUES_PATH.c_str());
        if(access(VALUES_PATH.c_str(), R_OK | W_OK)){
            creat(VALUES_PATH.c_str(), 0666);
        }
        qLogInfofmt("StartupConfigurator: %d Handlers..", HANDLER_THREADS);
        for (int i = 0; i < HANDLER_THREADS; i++) {
            recvaddres[i] = string(REQ_ADDR_PREFIX) + ItoS(i);
            rsaddr[i] = mksockaddr_un(recvaddres[i]);
        }
        qLogInfofmt("StartupConfigurator: %d UDSs..", UDS_NUM);
        for (int i = 0; i < UDS_NUM; i++) {
            int sv = requestfds[i].open();
            if (sv == -1) {
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
        qLogInfo("Startup: FORK !");
        if (fork()) {
            // parent
            qLogInfo("Startup: FORK completed.");
            qLogInfo("Startup: wait ReqHandler startup complete.");
            while (start_ok == false);
            qLogInfo("Startup: HeartBeat thread.");
            thread hbthread(HeartBeater, HB_ADDR, &running);
            hbthread.detach();
            qLogInfo("Startup: Everything OK.");
        } else {
            // child
            qLogInfo("RequestHandler: FORK completed.");
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
            requestfds[i].close();
        }
    }

// 3. Write a key-value pair into engine
    RetCode EngineRace::Write(const PolarString &key, const PolarString &value) {
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
        } while (reqfds_occupy[reqIdx % UDS_NUM].compare_exchange_strong(fk, true));
        // then OK, we do writing work
        ssize_t sv = requestfds[reqIdx % UDS_NUM].sendOne(
                reinterpret_cast<char *>(&rr), sizeof(RequestResponse), &(rsaddr[reqIdx % HANDLER_THREADS]));
        if (sv == -1) {
            return kIOError;
        }
        struct sockaddr_un useless;
        ssize_t rv = requestfds[reqIdx % UDS_NUM].getOne(
                reinterpret_cast<char *>(&rr), sizeof(RequestResponse), &useless);
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
        } while (reqfds_occupy[reqIdx % UDS_NUM].compare_exchange_strong(fk, true));
        // then OK, we do writing work
        ssize_t sv = requestfds[reqIdx % UDS_NUM].sendOne(
                reinterpret_cast<char *>(&rr), sizeof(RequestResponse), &(rsaddr[reqIdx % HANDLER_THREADS]));
        if (sv == -1) {
            return kIOError;
        }
        struct sockaddr_un useless;
        ssize_t rv = requestfds[reqIdx % UDS_NUM].getOne(
                reinterpret_cast<char *>(&rr), sizeof(RequestResponse), &useless);
        if (rv == -1) {
            return kIOError;
        }
        if (rr.type != RequestType::TYPE_OK) {
            return kNotFound;
        }
        *value = string(rr.value);
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
