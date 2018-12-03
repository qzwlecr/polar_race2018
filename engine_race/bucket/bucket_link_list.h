//
// Created by qzwlecr on 12/2/18.
//

#ifndef ENGINE_BUCKET_LINK_LIST_H
#define ENGINE_BUCKET_LINK_LIST_H

#include "consts/consts.h"
#include <vector>
#include <map>
#include <atomic>

namespace polar_race {

    class BucketLinkList {
    public:
        int id;
        std::vector<uint64_t> links;
        std::vector<uint64_t> sizes;
        static std::map<uint64_t, int> checker;

        BucketLinkList() : links(), sizes() {}

        BucketLinkList(int id) : id(id), links(), sizes() {}

        uint64_t get(uint64_t head); // get next slice head in this linked bucket

        static void persist(int fd);

        static int check_location(uint64_t offset);

        static void unpersist(int fd);

    };

    extern BucketLinkList *BucketLinkLists[BUCKET_NUMBER];
    extern std::atomic<uint64_t> NextIndex;
    extern int ValuesFd;
    extern int MetaFd;

}


#endif //ENGINE_BUCKET_LINK_LIST_H
