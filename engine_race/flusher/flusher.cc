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

    void Flusher::flush_begin() {
        thread flush_reader(&Flusher::read, this);
        flush_reader.detach();
        thread flush_executor(&Flusher::flush, this);
        flush_executor.detach();
        return;
    }

    void *Flusher::read() {
        for (uint64_t index = WrittenIndex % COMMIT_QUEUE_LENGTH;; index++) {
            if (index == COMMIT_QUEUE_LENGTH)
                index = 0;
            while (CommitCompletionQueue[index] == 0) {
                if (UNLIKELY(ExitSign)) {
                    last_flush = true;
                    return nullptr;
                }
            }
            if (internal_buffer_index == (INTERNAL_BUFFER_LENGTH / 2 / 4096) ||
                   internal_buffer_index == INTERNAL_BUFFER_LENGTH / 4096){
                flushing = true;
            }
            while(flushing);
            memcpy(InternalBuffer + internal_buffer_index * 4096,
                   CommitQueue + index * 4096,
                   4096);
            internal_buffer_index++;
            CommitCompletionQueue[index] = 0;
            qLogDebugfmt("Reader: %lu", internal_buffer_index);
        }

    }

    void *Flusher::flush() {
        int fd = open(VALUES_PATH.c_str(), O_RDWR | O_APPEND | O_SYNC | O_CREAT | O_DIRECT, 0666);
        qLogDebugfmt("Flusher: File Descripter=[%d]", fd);
        while (1) {
            while (!flushing && UNLIKELY(!ExitSign));
            qLogDebug("Flusher: Ready to flush to disk");
            if (UNLIKELY(ExitSign)) {
                while (!last_flush);
                if (WrittenIndex % INTERNAL_BUFFER_LENGTH) {
                    write(fd, InternalBuffer + (INTERNAL_BUFFER_LENGTH / 2),
                          INTERNAL_BUFFER_LENGTH / 2);
                    WrittenIndex += INTERNAL_BUFFER_LENGTH / 2;
                } else {
                    write(fd, InternalBuffer, INTERNAL_BUFFER_LENGTH);
                    WrittenIndex += INTERNAL_BUFFER_LENGTH;
                }
                int index_fd = open(INDECIES_PATH.c_str(), O_CREAT | O_TRUNC | O_RDWR | O_APPEND, 0666);
                global_index_store.persist(index_fd);
                flock(lockfd, LOCK_UN);
                qLogDebug("Flusher unlocked filelock");
                close(index_fd);
                close(fd);
                exit(0);
            }
            if (internal_buffer_index == INTERNAL_BUFFER_LENGTH / 4096) {
                internal_buffer_index = 0;
                flushing = false;
                qLogDebugfmt("Flusher: flush from %lu to %lu, with index %lu", INTERNAL_BUFFER_LENGTH/2, INTERNAL_BUFFER_LENGTH, internal_buffer_index);
                write(fd, InternalBuffer + (INTERNAL_BUFFER_LENGTH / 2),
                      INTERNAL_BUFFER_LENGTH / 2);
            } else {
                flushing = false;
                qLogDebugfmt("Flusher: flush from %lu to %lu, with index %lu", 0lu, INTERNAL_BUFFER_LENGTH/2, internal_buffer_index);
                write(fd, InternalBuffer, INTERNAL_BUFFER_LENGTH / 2);
            }
            WrittenIndex += INTERNAL_BUFFER_LENGTH / 2;
            qLogDebugfmt("Flusher: written index = %lu", WrittenIndex);
        }
    }
}
