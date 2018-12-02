//
// Created by qzwlecr on 12/2/18.
//

#ifndef ENGINE_BUCKET_LINK_LIST_H
#define ENGINE_BUCKET_LINK_LIST_H

#include "consts/consts.h"
#include <vector>
#include <atomic>

namespace polar_race {

    class BucketLinkList {
    public:
        int id;
        std::vector<uint64_t> links;
        std::vector<uint64_t> sizes;

        BucketLinkList() : links(), sizes() { sizes.push_back(0), links.push_back(0); }

        BucketLinkList(int id) : id(id), links(), sizes() { sizes.push_back(0), links.push_back(0); }

        uint64_t get(uint64_t head); // get next slice head in this linked bucket

        static void persist(int fd);

        static void unpersist(int fd);

    };

    extern BucketLinkList *BucketLinkLists[BUCKET_NUMBER];
    extern std::atomic<uint64_t> NextIndex;
    extern int ValuesFd;
    extern int MetaFd;

}


#endif //ENGINE_BUCKET_LINK_LIST_H
