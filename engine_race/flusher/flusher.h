//
// Created by qzwlecr on 10/31/18.
//

#ifndef ENGINE_RACE_FLUSHER_H
#define ENGINE_RACE_FLUSHER_H

#include "../format/log.h"
#include "../consts/consts.h"
#include "../task/task.h"

#include <cstddef>
#include <cstdint>
#include <thread>
#include <atomic>
#include <unistd.h>
#include <cstring>

extern "C" {
#include <unistd.h>
#include <fcntl.h>
#include <sys/file.h>
}

namespace polar_race {

    const uint8_t COMMIT_COMPLETION_EMPTY = 0;
    const uint8_t COMMIT_COMPLETION_OCCUPIED = 1;
    const uint8_t COMMIT_COMPLETION_FULL = 2;
    extern char *CommitQueue;
    extern std::atomic<uint8_t> *CommitCompletionQueue;
    extern volatile uint64_t WrittenIndex;
    extern char *InternalBuffer;

    class Flusher {
    public:
        Flusher();

        void flush_begin();

    private:
        void *read();

        void *flush();

        volatile uint64_t internal_buffer_index = 0;
        volatile bool last_flush = false;
        volatile bool flushing = false;

    };
}

#endif //ENGINE_RACE_FLUSHER_H
