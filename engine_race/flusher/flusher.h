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

namespace polar_race {

#if defined(__GNUC__)
#define LIKELY(x) (__builtin_expect((x), 1))
#define UNLIKELY(x) (__builtin_expect((x), 0))
#else
#define LIKELY(x) (x)
#define UNLIKELY(x) (x)
#endif

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
