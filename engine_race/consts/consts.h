#ifndef ENGINE_RACE_CONSTS_CONSTS_H
#define ENGINE_RACE_CONSTS_CONSTS_H

#include <cstdint>

namespace polar_race {
    const std::size_t KEY_SIZE = 8;
    const std::size_t VAL_SIZE = 4096;
    const int CONCURRENT_QUERY = 64;
    const int HANDLER_THREADS = 16;
};

#endif
