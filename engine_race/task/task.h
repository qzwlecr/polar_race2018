#ifndef ENGINE_RACE_TASK_H
#define ENGINE_RACE_TASK_H

#include <cstdint>
#include <string>

namespace polar_race {

	extern const uint64_t CommitQueueLength;
	extern char *CommitQueue;
	extern bool *CommitCompletionQueue;
	extern uint64_t WrittenIndex;
	extern char *InternalBuffer;
    extern const std::string VALUES_PATH;
    extern const std::string INDECIES_PATH;

    extern bool ExitSign; // on true, execute exit clean proc

    const std::string HB_ADDR = "\0ralopdb/heartbeat";
    const std::string REQ_ADDR_PREFIX = "\0ralopdb/request/";

    void RequestProcessor(std::string recvaddr);
    void HeartBeater(std::string sendaddr);
    void HeartBeatChecker(std::string recvaddr);

};

#endif
