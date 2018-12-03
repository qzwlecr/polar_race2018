#ifndef ENGINE_RACE_INDEX_INDEX_H
#define ENGINE_RACE_INDEX_INDEX_H

#include <cstdint>
#include "index/hashmap.hpp"

namespace polar_race {
    class IndexStore {
    public:
        bool put(uint64_t key, uint64_t offset);

        bool get(uint64_t key, uint64_t &offset);

        void persist(int fd);

        void unpersist(int fd);

        IndexStore();

        AtomicUnorderedInsertMap hashmap;
    };

    extern IndexStore *GlobalIndexStore;
};


#endif
