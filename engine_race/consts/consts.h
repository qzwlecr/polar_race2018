#ifndef ENGINE_RACE_CONSTS_CONSTS_H
#define ENGINE_RACE_CONSTS_CONSTS_H

#include <cstdint>
#include <string>
#include "../timer/timer.h"

namespace polar_race {
    enum ExecutionMode {
        MODE_MPROC_SEQ_WR = 0,
        MODE_MPROC_RAND_WR
    };
    const std::size_t KEY_SIZE = 8;
    const std::size_t VAL_SIZE = 4096;
    const int CONCURRENT_QUERY = 64;
    const int HANDLER_THREADS = 32;
    const float LOAD_FACTOR = 0.8f;
    const std::size_t HASH_MAP_SIZE = 64000000;
    const uint64_t COMMIT_QUEUE_LENGTH = 4096;
    const uint64_t INTERNAL_BUFFER_LENGTH = 2048 * 4096;
    const std::string VALUES_PATH_SUFFIX = "/ralopdb.dat";
    const std::string INDECIES_PATH_SUFFIX = "/ralopdb_index.dat";
    extern std::string VALUES_PATH;
    extern std::string INDECIES_PATH;
    const int UDS_CONGEST_AMPLIFIER = 2;
    const bool UDS_BIND_FAIL_SUPRESS = true;
    const bool SIGNAL_FULL_DUMP = true;
    const bool SELFCLOSER_ENABLED = false;
    const int SANITY_EXEC_TIME = 600;
    extern TimingProfile handtps[HANDLER_THREADS];
    const bool EXEC_MODE_BENCHMARK = false;
};

#if defined(__GNUC__)
#define LIKELY(x) (__builtin_expect((x), 1))
#define UNLIKELY(x) (__builtin_expect((x), 0))
#else
#define LIKELY(x) (x)
#define UNLIKELY(x) (x)
#endif

#endif
