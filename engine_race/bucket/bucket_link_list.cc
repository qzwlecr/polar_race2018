//
// Created by qzwlecr on 12/2/18.
//

#include "bucket_link_list.h"

extern "C" {
#include<unistd.h>
}

namespace polar_race {
    BucketLinkList *BucketLinkLists[BUCKET_NUMBER];
    std::atomic<uint64_t> NextIndex;
    std::map<uint64_t, int> BucketLinkList::checker;
    int ValuesFd;
    int MetaFd;

    uint64_t BucketLinkList::get(uint64_t head) {
        if (head + BUCKET_BUFFER_LENGTH >
            links.back() + (links.size() == 1 ? FIRST_BUCKET_LENGTH : OTHER_BUCKET_LENGTH)) {
            uint64_t next_head = NextIndex.fetch_add(OTHER_BUCKET_LENGTH);
            links.push_back(next_head);
            sizes.push_back(BUCKET_BUFFER_LENGTH / VAL_SIZE);
            return next_head;
        } else {
            sizes.back() += BUCKET_BUFFER_LENGTH / VAL_SIZE;
            return head + BUCKET_BUFFER_LENGTH;
        }
    }

    void BucketLinkList::unpersist(int fd) {
        size_t size;
        BucketLinkList::checker.clear();
        for (size_t i = 0; i < BUCKET_NUMBER; i++) {
            read(fd, &size, sizeof(size));
            uint64_t index, sizee;
            for (size_t j = 0; j < size; ++j) {
                read(fd, &index, sizeof(index));
                BucketLinkLists[i]->links.push_back(index);
                read(fd, &sizee, sizeof(sizee));
                BucketLinkLists[i]->sizes.push_back(sizee);
                BucketLinkList::checker.insert(std::make_pair(index, i));
            }
        }

    }

    void BucketLinkList::persist(int fd) {
        for (size_t i = 0; i < BUCKET_NUMBER; i++) {
            size_t size = BucketLinkLists[i]->links.size();
            write(fd, &size, sizeof(size_t));
            for (size_t j = 0; j < size; ++j) {
                write(fd, &(BucketLinkLists[i]->links[j]), sizeof(uint64_t));
                write(fd, &(BucketLinkLists[i]->sizes[j]), sizeof(uint64_t));
            }
        }
    }

    int BucketLinkList::check_location(uint64_t offset) {
        auto iter = checker.upper_bound(offset);
        --iter;
        return iter->second;
    }

}
