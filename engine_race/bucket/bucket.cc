//
// Created by qzwlecr on 12/1/18.
//

#include "bucket.h"
#include "bucket_link_list.h"
#include <cstring>

extern "C" {
#include <unistd.h>
#include <malloc.h>
}

namespace polar_race {
    Bucket *Buckets[BUCKET_NUMBER];
    char *BackupBuffer[BUCKET_BACKUP_NUMBER];
    std::atomic_bool BackupBufferU[BUCKET_BACKUP_NUMBER];
    std::atomic<uint8_t> BackupCount;
    thread_pool *BucketThreadPool;

    Bucket::Bucket(int id) : id(id) {
        size_t pagesize = (size_t) getpagesize();
        buffer = (char *) memalign(pagesize, BUCKET_BUFFER_LENGTH);
        return;
    }

    void Bucket::put(uint64_t &location, char *value) {
        uint64_t index = next_index.fetch_add(VAL_SIZE);
        if (index >= head_index + BUCKET_BUFFER_LENGTH) {
            uint64_t num = 0;
            bool desired;
            do {
                num = BackupCount.fetch_add(1);
                desired = false;
            } while (!BackupBufferU[num % BUCKET_BACKUP_NUMBER].compare_exchange_weak(desired, true));
            BucketThreadPool->execute(std::bind(flushBuffer, head_index, buffer, num));
            std::swap(buffer, BackupBuffer[num % BUCKET_BACKUP_NUMBER]);
            index = BucketLinkLists[id]->get(head_index);
        }
        memcpy(buffer + index, value, VAL_SIZE);
        location = index;
    }

    Bucket::~Bucket() {
        free(buffer);
    }

    void flushBuffer(uint64_t index, char *buffer, int should_done) {
        pwrite(ValuesFd, buffer, BUCKET_BUFFER_LENGTH, index);
        BackupBufferU[should_done] = false;
    }


}
