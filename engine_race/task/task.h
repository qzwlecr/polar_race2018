#ifndef ENGINE_RACE_TASK_H
#define ENGINE_RACE_TASK_H

#include <cstdint>

namespace polar_race {

	extern const uint64_t CommitQueueLength;
	extern char *CommitQueue;
	extern bool *CommitCompletionQueue;
	extern uint64_t WrittenIndex;
	extern char *InternalBuffer;


};

#endif
