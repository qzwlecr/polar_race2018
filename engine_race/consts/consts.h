#ifndef ZHWK_POLARDB_RACE_CONSTS_H
#define ZHWK_POLARDB_RACE_CONSTS_H

#include <cstdint>

namespace ZHWKLib {
    const std::size_t KEY_SIZE = 8;
    const std::size_t VAL_SIZE = 4096;
    const int CONCURRENT_QUERY = 64;
    const int HANDLER_THREADS = 16;
};

#endif
