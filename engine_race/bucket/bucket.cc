//
// Created by qzwlecr on 12/1/18.
//

#include "bucket.h"
#include "bucket_link_list.h"
#include "format/log.h"
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

    Bucket::Bucket(int id) : head_index(0), next_index(0), id(id), done_number(0) {
        size_t pagesize = (size_t) getpagesize();
        buffer = (char *) memalign(pagesize, BUCKET_BUFFER_LENGTH);
        return;
    }

    void Bucket::put(uint64_t &location, char *value) {
        WRITING_BEGIN:
        uint64_t index = next_index.fetch_add(VAL_SIZE);
        if (index >= head_index + BUCKET_BUFFER_LENGTH) {
            qLogDebug("Bucket::put: getting writing lock before");
            writing.lock();
            qLogDebug("Bucket::put: getting writing lock after");
            if (index < head_index + BUCKET_BUFFER_LENGTH) {
                writing.unlock();
                qLogDebug("Bucket::put: writing work is already done by other threads");
                goto WRITING_BEGIN;
            }
            qLogDebug("Bucket::put: waiting for all before");
            while (done_number != BUCKET_BUFFER_LENGTH / VAL_SIZE);
            qLogDebug("Bucket::put: waiting for all after");
            uint64_t num = 0;
            bool desired;
            do {
                num = BackupCount.fetch_add(1);
                desired = false;
            } while (!BackupBufferU[num % BUCKET_BACKUP_NUMBER].compare_exchange_weak(desired, true));
            qLogDebug("Bucket::put: getting backup buffer");
            std::swap(buffer, BackupBuffer[num % BUCKET_BACKUP_NUMBER]);
            uint64_t last_head_index = head_index;
            head_index = BucketLinkLists[id]->get(head_index);
            next_index = head_index;
            done_number = 0;
            qLogInfofmt("Bucket::put: last head index = %lu, head index = %lu,  num = %lu", last_head_index,
                         head_index, num);
            writing.unlock();
            flushBuffer(last_head_index, BackupBuffer[num % BUCKET_BACKUP_NUMBER], int(num % BUCKET_BACKUP_NUMBER));
            goto WRITING_BEGIN;
            //TODO: if bug occurs, check if all backup buffers are already used.
        }
        memcpy(buffer + index - head_index, value, VAL_SIZE);
        qLogDebugfmt("Bucket::put: memcpy done from %lx", (uint64_t) (buffer + index - head_index));
        done_number.fetch_add(1);
        location = index;
    }

    Bucket::~Bucket() {
        flushBuffer(head_index, buffer, 0);
        free(buffer);
    }

    void flushBuffer(uint64_t index, char *buffer, int should_done) {
        qLogInfofmt("FlushBuffer: Write %lu %lx %d", index, (uint64_t) buffer, should_done);
        if (pwrite(ValuesFd, buffer, BUCKET_BUFFER_LENGTH, index) == -1) {
            qLogFail(strerror(errno));
        };
        BackupBufferU[should_done] = false;
    }


}
