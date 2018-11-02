//
// Created by qzwlecr on 10/31/18.
//

#include <flusher/flusher.h>
#include <index/index.h>

namespace polar_race {
    using namespace std;
    char CommitQueue[COMMIT_QUEUE_LENGTH * 4096];
    volatile bool CommitCompletionQueue[COMMIT_QUEUE_LENGTH];
    uint64_t WrittenIndex;
    char *InternalBuffer;
    char RealInternalBuffer[INTERNAL_BUFFER_LENGTH * 2];

    void Flusher::flush_begin() {
        InternalBuffer = RealInternalBuffer;
        thread flush_reader(&Flusher::read, this);
        flush_reader.detach();
        thread flush_executor(&Flusher::flush, this);
        flush_executor.detach();
        return;
    }

    void *Flusher::read() {
        for (uint64_t index = 0;; index++) {
            if (index == COMMIT_QUEUE_LENGTH)
                index = 0;
            while (CommitCompletionQueue[index] == 0) {
                if (UNLIKELY(ExitSign)) {
                    last_flush = true;
                    return nullptr;
                }
            }
            while (internal_buffer_index == INTERNAL_BUFFER_LENGTH);
            memcpy(InternalBuffer + internal_buffer_index * 4096,
                   CommitQueue + index * 4096,
                   4096);
            internal_buffer_index++;
            CommitCompletionQueue[index] = 0;
        }

    }

    void *Flusher::flush() {
        int fd = open(VALUES_PATH.c_str(), O_RDWR | O_APPEND | O_SYNC | O_CREAT | O_DIRECT);
        while (1) {
            while (internal_buffer_index != INTERNAL_BUFFER_LENGTH && !UNLIKELY(ExitSign));
            if (UNLIKELY(ExitSign)) {
                while (!last_flush);
                write(fd, InternalBuffer, internal_buffer_index * 4096);
                WrittenIndex += internal_buffer_index;
                int index_fd = open(INDECIES_PATH.c_str(), O_CREAT | O_TRUNC | O_RDWR);
                global_index_store.persist(index_fd);
                close(index_fd);
                close(fd);
                exit(0);
            }
            internal_buffer_part = 1 - internal_buffer_part;
            InternalBuffer = RealInternalBuffer + internal_buffer_part * INTERNAL_BUFFER_LENGTH;
            internal_buffer_index = 0;
            write(fd, RealInternalBuffer + (1 - internal_buffer_part) * INTERNAL_BUFFER_LENGTH, INTERNAL_BUFFER_LENGTH);
            WrittenIndex += INTERNAL_BUFFER_LENGTH;
        }
    }
}
