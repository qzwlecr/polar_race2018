#include "index.h"
#include "hashmap.h"

namespace polar_race {
	class IndexStore {
	public:
		bool put(uint64_t key, uint64_t offset);

		bool get(uint64_t key, uint64_t &offset);

		IndexStore();

		~IndexStore();
	};
};
