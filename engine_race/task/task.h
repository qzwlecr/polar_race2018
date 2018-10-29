#ifndef ZHWK_POLARDB_RACE_TASK_H
#define ZHWK_POLARDB_RACE_TASK_H

#include <cstdint>

namespace ZHWKLib {

    extern const uint64_t CommitQueueLength;
    extern char* CommitQueue;
    extern bool* CommitCompletionQueue;
    extern uint64_t WrittenIndex;
    extern char* InternalBuffer;

    

};

#endif
