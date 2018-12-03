#include "index/index.h"
#include "consts/consts.h"
#include "format/log.h"

namespace polar_race {
    IndexStore *GlobalIndexStore;
    OffsetTable *GlobalOffsetTable;
    std::mt19937 eng;
    std::uniform_int_distribution<uint32_t> uid;

    bool IndexStore::put(uint64_t key, uint64_t offset) {

        auto ret = hashmap.findOrConstruct(key, [&](void *raw) {
            new(raw) MutableData(uint32_t(offset / VAL_SIZE));
        });

        qLogDebugfmt("IndexStore: put %lu into %lu", offset, key);
        if (UNLIKELY(!ret.second)) {
            (*(ret.first)).second.data = (uint32_t) (offset / VAL_SIZE);
        }
        return true;
    }

    bool IndexStore::get(uint64_t key, uint64_t &offset) {
        auto ret = hashmap.find(key, hashmap.keyToSlotIdx(key));
        if (UNLIKELY(ret == 0)) {
            return false;
        } else {
            offset = hashmap.slots_[ret].keyValue().second.data * VAL_SIZE;
            qLogDebugfmt("IndexStore: get %lu from %lu", offset, key);
            return true;
        }

    }

    IndexStore::IndexStore() : hashmap(HASH_MAP_SIZE, LOAD_FACTOR) {
        std::random_device r;
        eng = std::mt19937(r());
        uid = std::uniform_int_distribution<uint32_t>(0, uint32_t(HASH_MAP_SIZE / LOAD_FACTOR + 128));
    }

    void IndexStore::persist(int fd) {
        write(fd, hashmap.slots_, hashmap.allocator_.computeSize(hashmap.mmapRequested_));
        return;
    }

    void IndexStore::unpersist(int fd) {
        read(fd, hashmap.slots_, hashmap.allocator_.computeSize(hashmap.mmapRequested_));
        return;
    }
};
