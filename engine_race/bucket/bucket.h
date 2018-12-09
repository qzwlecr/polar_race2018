//
// Created by qzwlecr on 12/1/18.
//

#ifndef ENGINE_BUCKET_H
#define ENGINE_BUCKET_H

#include "consts/consts.h"
#include <atomic>
#include <mutex>
#include <vector>

namespace polar_race {
    class Bucket {
    public:

        Bucket(int id);

        void put(uint64_t &location, char *value);

        volatile uint64_t head_index;

        std::atomic<uint64_t> next_index;

        ~Bucket();

    private:

        int id;

        std::mutex flushing;

        std::mutex loading;

        std::atomic<uint64_t> done_number;

        char *buffer;

    };

    void flushBuffer(uint64_t index, char *buffer, int should_done);

    extern Bucket *Buckets[BUCKET_NUMBER];
    extern char *BackupBuffer[BUCKET_BACKUP_NUMBER];
    extern std::atomic_bool BackupBufferU[BUCKET_BACKUP_NUMBER];
    extern std::atomic<uint8_t> BackupCount;

}


#endif //ENGINE_BUFFER_H
