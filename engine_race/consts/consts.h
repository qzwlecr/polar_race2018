#ifndef ENGINE_RACE_CONSTS_CONSTS_H
#define ENGINE_RACE_CONSTS_CONSTS_H

#include <cstdint>
#include <string>
#include "../timer/timer.h"

namespace polar_race {
    const std::size_t KEY_SIZE = 8;
    const std::size_t VAL_SIZE = 4096;
    const int CONCURRENT_QUERY = 64;
    const int HANDLER_THREADS = 16;
	const std::size_t BUFFER_SIZE = 32768;
	const std::size_t HASH_MAP_SIZE = 64000000;
	const uint64_t COMMIT_QUEUE_LENGTH = 4096;
	const uint64_t INTERNAL_BUFFER_LENGTH = 4096 * 4096;
	const std::string VALUES_PATH_SUFFIX = "/ralopdb.dat";
	const std::string INDECIES_PATH_SUFFIX = "/ralopdb_index.dat";
    extern std::string VALUES_PATH;
    extern std::string INDECIES_PATH;
    const int UDS_CONGEST_AMPLIFIER = 2;
    const bool UDS_BIND_FAIL_SUPRESS = true;
    const bool SIGNAL_FULL_DUMP = true;
    extern TimingProfile handtps[HANDLER_THREADS];
};

#endif
