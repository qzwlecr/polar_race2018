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
    const std::size_t FILE_SIZE = 1ul * 1024 * 1024 * 1024;
    const std::size_t KEY_SIZE = 8;
    const std::size_t VAL_SIZE = 4096;
    const int CONCURRENT_QUERY = 1;
    const int HANDLER_THREADS = 2;
    const float LOAD_FACTOR = 0.9f;
    const std::size_t HASH_MAP_SIZE = 640000;
    const int BUCKET_NUMBER = 64;
    const int BUCKET_BUFFER_LENGTH = 128 * 4096;
    const int BUCKET_BACKUP_NUMBER = 64;
    const int FIRST_BUCKET_LENGTH = 4 * 1024 * 1024;
    const int OTHER_BUCKET_LENGTH = 1 * 1024 * 1024;
    const uint64_t COMMIT_QUEUE_LENGTH = 4096;
    const uint64_t INTERNAL_BUFFER_LENGTH = 2048 * 4096;
    const std::string VALUES_PATH_SUFFIX = "/ralopdb.dat";
    const std::string INDECIES_PATH_SUFFIX = "/ralopdb_index.dat";
    const std::string META_PATH_SUFFIX = "/ralopdb_meta.dat";
    const std::string OFFSET_TABLE_PATH_SUFFIX = "/ralopdb_offsets.dat";
    const std::string KEY_TABLE_PATH_SUFFIX = "/ralopdb_keys.dat";
    extern std::string VALUES_PATH;
    extern std::string INDECIES_PATH;
    extern std::string META_PATH;
    extern std::string OFFSET_TABLE_PATH;
    extern std::string KEY_TABLE_PATH;
    const int UDS_CONGEST_AMPLIFIER = 2;
    const bool UDS_BIND_FAIL_SUPRESS = true;
    const bool SIGNAL_FULL_DUMP = true;
    const bool SELFCLOSER_ENABLED = false;
    const int SANITY_EXEC_TIME = 600;
    extern TimingProfile handtps[HANDLER_THREADS];
    const bool EXEC_MODE_BENCHMARK = false;
    const uint32_t RANGE_CACHE_SIZE = 1 * 1024 * 1024 * 1024;
    const int AIO_WR_QDEPTH = 32;
};

#if defined(__GNUC__)
#define LIKELY(x) (__builtin_expect((x), 1))
#define UNLIKELY(x) (__builtin_expect((x), 0))
#else
#define LIKELY(x) (x)
#define UNLIKELY(x) (x)
#endif

#endif
