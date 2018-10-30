#ifndef ENGINE_RACE_INDEX_INDEX_H
#define ENGINE_RACE_INDEX_INDEX_H

#include <cstdint>
#include "hashmap.h"

namespace polar_race {
	class IndexStore {
	public:
		bool put(uint64_t key, uint64_t offset);

		bool get(uint64_t key, uint64_t &offset);

		IndexStore();

		~IndexStore();

	private:
		AtomicUnorderedInsertMap<uint64_t, MutableAtom<uint64_t>> hashmap;
	};
};


#endif