//
// Created by qzwlecr on 10/31/18.
//

#include <flusher/flusher.h>
#include <index/index.h>

namespace polar_race {
    using namespace std;
    char CommitQueue[COMMIT_QUEUE_LENGTH * 4096];
    volatile bool CommitCompletionQueue[COMMIT_QUEUE_LENGTH];
    volatile uint64_t WrittenIndex = 0;
    char *InternalBuffer;
    char *RealInternalBuffer;

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
            while (internal_buffer_index == INTERNAL_BUFFER_LENGTH / 4096);
            memcpy(InternalBuffer + internal_buffer_index * 4096,
                   CommitQueue + index * 4096,
                   4096);
            internal_buffer_index++;
            CommitCompletionQueue[index] = 0;
            qLogDebugfmt("Reader: %lu", internal_buffer_index);
        }

    }

    void *Flusher::flush() {
        int fd = open(VALUES_PATH.c_str(), O_RDWR | O_APPEND | O_SYNC | O_CREAT | O_DIRECT);
        qLogDebugfmt("Flusher: File Descripter=[%d]", fd);
        while (1) {
            while (internal_buffer_index != INTERNAL_BUFFER_LENGTH / 4096 && UNLIKELY(!ExitSign));
            qLogDebug("Flusher: Ready to flush to disk");
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
            qLogDebugfmt("Flusher: Flushing to disk, part=[%d], index=[%lu]", internal_buffer_part,
                         internal_buffer_index);
            internal_buffer_index = 0;

            ssize_t ret = write(fd, RealInternalBuffer + (1 - internal_buffer_part) * INTERNAL_BUFFER_LENGTH,
                                INTERNAL_BUFFER_LENGTH);
            if (ret == -1) {
                char *err = strerror(errno);
                qLogDebugfmt("Flusher: Can't write to disk, %s", err);
            }
            WrittenIndex += INTERNAL_BUFFER_LENGTH;
        }
    }
}
