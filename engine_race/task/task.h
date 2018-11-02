#ifndef ENGINE_RACE_TASK_H
#define ENGINE_RACE_TASK_H

#include <cstdint>
#include <string>
#include "../commu/commu.h"

namespace polar_race {

    extern char CommitQueue[COMMIT_QUEUE_LENGTH * 4096];
    extern bool CommitCompletionQueue[COMMIT_QUEUE_LENGTH];
    extern uint64_t WrittenIndex;
    extern char *InternalBuffer;

    extern bool ExitSign; // on true, execute exit clean proc

    extern Accumulator NextIndex;

    const std::string HB_ADDR = (std::string() + '\0') + "ralopdb/heartbeat";
    const std::string REQ_ADDR_PREFIX = (std::string() + '\0') + "ralopdb/request/";

    void RequestProcessor(std::string recvaddr);

    void HeartBeater(std::string sendaddr, bool *runstate);

    void HeartBeatChecker(std::string recvaddr);

};

#endif
