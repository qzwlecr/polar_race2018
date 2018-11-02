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

namespace polar_race {

#if defined(__GNUC__)
#define LIKELY(x) (__builtin_expect((x), 1))
#define UNLIKELY(x) (__builtin_expect((x), 0))
#else
#define LIKELY(x) (x)
#define UNLIKELY(x) (x)
#endif
    extern bool ExitSign;

    class Flusher {
    public:
        void flush_begin();

    private:
        void *read();

        void *flush();

        volatile uint64_t internal_buffer_index = 0;
        volatile bool last_flush = false;
        uint32_t internal_buffer_part = 0;

    };
}

#endif //ENGINE_RACE_FLUSHER_H
