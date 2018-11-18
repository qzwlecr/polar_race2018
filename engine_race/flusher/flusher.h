//
// Created by qzwlecr on 10/31/18.
//

#ifndef ENGINE_RACE_FLUSHER_H
#define ENGINE_RACE_FLUSHER_H

#include "format/log.h"
#include "consts/consts.h"
#include "task/task.h"

#include <cstddef>
#include <thread>
#include <unistd.h>
#include <cstring>
#include <fcntl.h>

extern "C" {
#include <sys/file.h>
}

namespace polar_race {

    extern char *CommitQueue;
    extern volatile bool *CommitCompletionQueue;
    extern volatile uint64_t WrittenIndex;
    extern char *InternalBuffer;

    class Flusher {
    public:
        Flusher();
        void flush_begin();

    private:
        int flush_start;
        int flush_end;
        void *read();

        void *flush();

        volatile uint64_t internal_buffer_index = 0;
        volatile bool last_flush = false;

    };
}

#endif //ENGINE_RACE_FLUSHER_H
