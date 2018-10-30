#include "index.h"

const int BUFFER_SIZE = 32768;
const int HASH_MAP_SIZE = 64000000;

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

	IndexStore::IndexStore() : hashmap(HASH_MAP_SIZE) {
		//for (;;) {
		// TODO: Load index from disk
		//}
	}

	IndexStore::~IndexStore() {

	}

	void IndexStore::persist(int fd) {
		uint64_t index = 0;
		char store[BUFFER_SIZE];
		uint64_t key, value;
		for (auto iter = hashmap.cbegin(); iter != hashmap.cend(); iter++) {
			key = (*iter).first, value = (*iter).second.data.load();
			if (index == BUFFER_SIZE) {
				write(fd, store, BUFFER_SIZE);
				index = 0;
			}
			memcpy(store + index, &key, 8);
			index += 8;
			memcpy(store + index, &value, 8);
			index += 8;
		}
		return;
	}

	void IndexStore::unpersist(int fd) {
		char store[BUFFER_SIZE];
		uint64_t key, value;
		ssize_t ret = 0;
		while ((ret = read(fd, store, BUFFER_SIZE)) > 0) {
			for (auto index = 0; index < ret;) {
				memcpy(&key, store + index, 8);
				index += 8;
				memcpy(&value, store + index, 8);
				index += 8;
				this->put(key, value);
			}
		}
		assert(ret > 0);
		return;
	}
};
