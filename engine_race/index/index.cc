#include <consts/consts.h>
#include <index/index.h>

namespace polar_race {
    IndexStore global_index_store;
    std::mt19937 eng;
    std::uniform_int_distribution<uint32_t> uid;

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
        std::random_device r;
        eng = std::mt19937(r());
        uid = std::uniform_int_distribution<uint32_t>(0, uint32_t(HASH_MAP_SIZE / 0.8f + 128));
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
        if (index != 0) {
            write(fd, store, index);
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
