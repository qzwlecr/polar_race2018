//
// Created by qzwlecr on 12/2/18.
//

#ifndef ENGINE_BUCKET_LINK_LIST_H
#define ENGINE_BUCKET_LINK_LIST_H

#include "consts/consts.h"
#include <vector>
#include <atomic>

namespace polar_race {
    extern std::atomic<uint64_t> NextIndex;

    class BucketLinkList {
    public:
        int id;
        size_t size;
        std::vector<uint64_t> links;

        BucketLinkList() : size(0), links() {}

        BucketLinkList(int id) : id(id), size(0), links() {}

        uint64_t get(uint64_t head); // get next slice head in this linked bucket

        static void persist(int fd);

        static void unpersist(int fd);

    };

    extern BucketLinkList* BucketLinkLists[BUCKET_NUMBER];

}


#endif //ENGINE_BUCKET_LINK_LIST_H
