#ifndef ENGINE_RACE_CONSTS_CONSTS_H
#define ENGINE_RACE_CONSTS_CONSTS_H

#include <cstdint>
#include <string>

namespace polar_race {
    const std::size_t KEY_SIZE = 8;
    const std::size_t VAL_SIZE = 4096;
    const int CONCURRENT_QUERY = 64;
    const int HANDLER_THREADS = 16;
	const int BUFFER_SIZE = 32768;
	const int HASH_MAP_SIZE = 64000000;
	const uint64_t COMMIT_QUEUE_LENGTH = 1024;
	const std::string VALUES_PATH = "./ralopdb.dat";
	const std::string INDECIES_PATH = "./ralopdb_index.dat";

};

#endif
