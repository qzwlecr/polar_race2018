#include "index.h"

namespace polar_race {
	bool IndexStore::put(uint64_t key, uint64_t offset) {

		auto ret = hashmap.findOrConstruct(key, [&](void *raw) {
			new(raw) MutableAtom<uint64_t>(offset);
		});
		if (UNLIKELY(ret.second)) {
			(*(ret.first)).second.data.store(offset);
		}
		return true;
	}

	bool IndexStore::get(uint64_t key, uint64_t &offset) {
		auto ret = hashmap.find(key, hashmap.keyToSlotIdx(key));
		if (UNLIKELY(ret == 0)) {
			return false;
		} else {
			offset = hashmap.slots_[ret].keyValue().second.data.load();
			return true;
		}

	}

	IndexStore::IndexStore() : hashmap(64000000) {
		//for (;;) {
		// TODO: Load index from disk
		//}
	}

	IndexStore::~IndexStore() {
	}
};
