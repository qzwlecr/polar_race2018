#ifndef ZHWK_POLARDB_RACE_INDEX_H
#define ZHWK_POLARDB_RACE_INDEX_H

#include <cstdint>

namespace ZHWKLib {
    class IndexStore {
        public:
            virtual bool put(uint64_t key, uint64_t offset) = 0;
            virtual bool get(uint64_t key, uint64_t& offset) = 0;
            virtual ~IndexStore() = 0;
    };
};


#endif
