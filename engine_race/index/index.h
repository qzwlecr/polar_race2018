#ifndef ENGINE_RACE_INDEX_INDEX_H
#define ENGINE_RACE_INDEX_INDEX_H

#include <cstdint>
#include "consts/consts.h"
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

#define REAL_OFFSET(x) (VAL_SIZE * 1ul * x)

    class OffsetTable {
    public:
        OffsetTable(int fd) {
            data = new uint32_t[HASH_MAP_SIZE];
            read(fd, data, HASH_MAP_SIZE * sizeof(uint32_t));
        };

        ~OffsetTable() { delete[]data; };
        uint32_t *data;
    };

    class KeyTable {
    public:
        KeyTable(int fd) {
            data = new uint64_t[HASH_MAP_SIZE];
            read(fd, data, HASH_MAP_SIZE * sizeof(uint64_t));
        };

        ~KeyTable() { delete[]data; };
        uint64_t *data;
    };

    extern IndexStore *GlobalIndexStore;
    extern OffsetTable *GlobalOffsetTable;
    extern KeyTable *GlobalKeyTable;
};


#endif
