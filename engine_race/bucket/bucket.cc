//
// Created by qzwlecr on 12/1/18.
//

#include "bucket.h"
#include "bucket_link_list.h"
#include "format/log.h"
#include "aio/aio.h"
#include <cstring>

extern "C" {
#include <unistd.h>
#include <malloc.h>
}

namespace polar_race {
    Bucket *Buckets[BUCKET_NUMBER];
    char *BackupBuffer[BUCKET_BACKUP_NUMBER];
    std::atomic_bool BackupBufferU[BUCKET_BACKUP_NUMBER];
    AIOQuest aioWriteQuests[BUCKET_BACKUP_NUMBER];
    std::atomic<uint8_t> BackupCount;

    Bucket::Bucket(int id) : head_index(0), next_index(0), id(id), done_number(0) {
        size_t pagesize = (size_t) getpagesize();
        buffer = (char *) memalign(pagesize, BUCKET_BUFFER_LENGTH);
        return;
    }

    void Bucket::put(uint64_t &location, char *value) {
        PUT_BEGIN:
        loading.lock();
        uint64_t last_head_index = head_index;
        uint64_t index = next_index.fetch_add(VAL_SIZE);
        loading.unlock();
        if (index >= last_head_index + BUCKET_BUFFER_LENGTH) {
            qLogDebugfmt("Bucket[%d]::put: getting writing lock before", id);
            flushing.lock();
            qLogDebugfmt("Bucket[%d]::put: getting writing lock after", id);

            if (head_index != last_head_index) {
                flushing.unlock();
                qLogInfofmt("Bucket[%d]::put: writing work is already done by other threads", id);
                goto PUT_BEGIN;
            }

            qLogInfofmt("Bucket[%d]::put: waiting for all before, done_number = %lu", id, done_number.load());
            while (done_number != BUCKET_BUFFER_LENGTH / VAL_SIZE);
            done_number = 0;
            qLogDebugfmt("Bucket[%d]::put: waiting for all after", id);

            uint64_t num = 0;
            bool desired;
            do {
                num = BackupCount.fetch_add(1);
                desired = false;
            } while (!BackupBufferU[num % BUCKET_BACKUP_NUMBER].compare_exchange_weak(desired, true));
            qLogDebugfmt("Bucket[%d]::put: getting backup buffer", id);
            std::swap(buffer, BackupBuffer[num % BUCKET_BACKUP_NUMBER]);
            uint64_t last_head_index = head_index;
            uint64_t next_head_index = BucketLinkLists[id]->get(head_index);
            loading.lock();
            head_index = next_head_index;
            next_index = next_head_index;
            loading.unlock();
            // ensure when next_index changes, head_index has already changed
            qLogInfofmt("Bucket[%d]::put: last head index = %lu, next head index = %lu,  num = %lu", id,
                        last_head_index,
                        next_head_index, num);
            flushing.unlock();
            AIOQuest *wactx = new(&(aioWriteQuests[num % BUCKET_BACKUP_NUMBER]))
                    AIOWriteQuest((uint32_t) ValuesFd, (uintptr_t) BackupBuffer[num % BUCKET_BACKUP_NUMBER],
                                  BUCKET_BUFFER_LENGTH, last_head_index);
            wactx->cb.aio_data = uint64_t(num % BUCKET_BACKUP_NUMBER);
            actx.submit(*wactx);
            goto PUT_BEGIN;
            //TODO: if bug occurs, check if all backup buffers are already used.
        }
        memcpy(buffer + index - last_head_index, value, VAL_SIZE);
        qLogDebugfmt("Bucket[%d]::put: memcpy done from %lx", id, (uint64_t) (buffer + index - last_head_index));
        done_number.fetch_add(1);
        location = index;
    }

    Bucket::~Bucket() {
        AIOQuest *wactx = new AIOWriteQuest((uint32_t) ValuesFd, (uintptr_t) buffer,
                                            BUCKET_BUFFER_LENGTH, head_index);
        wactx->cb.aio_data = 0;
        actx.submit(*wactx);
        free(buffer);
    }


    void flushDone(uint64_t data) {
        BackupBufferU[data] = false;
    }


}
