//
// Created by qzwlecr on 12/2/18.
//

#include "bucket_link_list.h"

extern "C" {
#include<unistd.h>
}

namespace polar_race {
    std::atomic<uint64_t> NextIndex;
    BucketLinkList *BucketLinkLists[BUCKET_NUMBER];

    uint64_t BucketLinkList::get(uint64_t head) {
        if (head + BUCKET_BUFFER_LENGTH >
            links.back() + (links.size() == 1 ? FIRST_BUCKET_LENGTH : OTHER_BUCKET_LENGTH)) {
            uint64_t next_head = NextIndex.fetch_add(OTHER_BUCKET_LENGTH);
            links.push_back(next_head);
            return next_head;
        } else {
            return head + BUCKET_BUFFER_LENGTH;
        }
    }

    void BucketLinkList::unpersist(int fd) {
        int size;
        for (int i = 0; i < BUCKET_NUMBER; i++) {
            read(fd, &size, sizeof(size));
            uint64_t index;
            for (int j = 0; j < size; ++j) {
                read(fd, &index, sizeof(index));
                BucketLinkLists[i]->links.push_back(index);
            }
        }

    }

    void BucketLinkList::persist(int fd) {
        for (int i = 0; i < BUCKET_NUMBER; i++) {
            write(fd, &(BucketLinkLists[i]->size), sizeof(size));
            for (size_t j = 0; j < BucketLinkLists[i]->size; ++j) {
                write(fd, &(BucketLinkLists[i]->links[j]), sizeof(uint64_t));
            }
        }

    }

}
