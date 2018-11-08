//
// Created by qzwlecr on 10/31/18.
//

#ifndef ENGINE_RACE_FLUSHER_H
#define ENGINE_RACE_FLUSHER_H

#include <cstddef>
#include <thread>
#include <unistd.h>
#include <cstring>
#include <consts/consts.h>
#include <fcntl.h>
#include <format/log.h>
#include <task/task.h>

extern "C"{
#include <sys/file.h>
}

namespace polar_race {

    extern char CommitQueue[COMMIT_QUEUE_LENGTH * VAL_SIZE];
    extern volatile bool CommitCompletionQueue[COMMIT_QUEUE_LENGTH];
    extern volatile uint64_t WrittenIndex;
    extern char *InternalBuffer;

    class Flusher {
    public:
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
