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

    extern bool ExitSign; // on true, execute exit clean proc

    void RequestProcessor(std::string recvaddr);
    void HeartBeater(std::string sendaddr);
    void HeartBeatChecker(std::string recvaddr);

};

#endif
