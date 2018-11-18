//
// Created by qzwlecr on 10/31/18.
//

#include "flusher/flusher.h"
#include "index/index.h"

extern "C" {
#include <malloc.h>
#include <sys/ipc.h>
#include <sys/sem.h>
}

namespace polar_race {
    using namespace std;
    char *CommitQueue;
    volatile bool *CommitCompletionQueue;
    volatile uint64_t WrittenIndex = 0;
    char *InternalBuffer;

    struct sembuf sem_buf_up{
            .sem_num = 0,
            .sem_op = 1
    };
    struct sembuf sem_buf_down{
            .sem_num = 0,
            .sem_op = -1
    };

    Flusher::Flusher() {
        size_t pagesize = (size_t) getpagesize();
        CommitQueue = (char *) memalign(pagesize, COMMIT_QUEUE_LENGTH * VAL_SIZE);
        CommitCompletionQueue = (volatile bool *) memalign(pagesize, COMMIT_QUEUE_LENGTH);
        InternalBuffer = (char *) memalign(pagesize, INTERNAL_BUFFER_LENGTH);
        if (CommitQueue == nullptr || CommitCompletionQueue == nullptr || InternalBuffer == nullptr) {
            qLogFailfmt("Flusher: allocating memory error %s", strerror(errno));
            abort();
        }
        memset(CommitQueue, 0, COMMIT_QUEUE_LENGTH * VAL_SIZE);
        memset((void *) CommitCompletionQueue, 0, COMMIT_QUEUE_LENGTH);
        memset(InternalBuffer, 0, INTERNAL_BUFFER_LENGTH);

    }

    void Flusher::flush_begin() {
        flush_start = semget(IPC_PRIVATE, 1, 0666 | IPC_CREAT);
        if (flush_start == -1) {
            qLogFailfmt("Startup: Acquiring semophore failed: %s", strerror(errno));
            abort();
        }
        if (semctl(flush_start, 0, SETVAL, 0) == -1) {
            qLogFailfmt("Startup: Set semophore failed: %s", strerror(errno));
            abort();
        }
        flush_end = semget(IPC_PRIVATE, 1, 0666 | IPC_CREAT);
        if (flush_end == -1) {
            qLogFailfmt("Startup: Acquiring semophore failed: %s", strerror(errno));
            abort();
        }
        if (semctl(flush_end, 0, SETVAL, 0) == -1) {
            qLogFailfmt("Startup: Set semophore failed: %s", strerror(errno));
            abort();
        }
        thread flush_reader(&Flusher::read, this);
        flush_reader.detach();
        thread flush_executor(&Flusher::flush, this);
        flush_executor.detach();
        return;
    }

    void *Flusher::read() {
        for (uint64_t index = (WrittenIndex / VAL_SIZE) % COMMIT_QUEUE_LENGTH;; index++) {
            if (index == COMMIT_QUEUE_LENGTH)
                index = 0;
            qLogDebugfmt("Flusher: index = %lu, WrittenIndex = %lu", index, WrittenIndex);
            while (CommitCompletionQueue[index] == 0) {
                if (UNLIKELY(ExitSign)) {
                    last_flush = true;
                    semop(flush_start, &sem_buf_up, 1);
                    return nullptr;
                }
            }

            if (internal_buffer_index == (INTERNAL_BUFFER_LENGTH / 2 / VAL_SIZE) ||
                internal_buffer_index == INTERNAL_BUFFER_LENGTH / VAL_SIZE) {
                semop(flush_start, &sem_buf_up, 1);
                semop(flush_end, &sem_buf_down, 1);
            }
            memcpy(InternalBuffer + internal_buffer_index * VAL_SIZE,
                   CommitQueue + index * VAL_SIZE,
                   VAL_SIZE);
            internal_buffer_index++;
            CommitCompletionQueue[index] = 0;
            qLogDebugfmt("Reader: %lu", internal_buffer_index);
        }

    }

    void *Flusher::flush() {
        int fd = open(VALUES_PATH.c_str(), O_RDWR | O_APPEND | O_SYNC | O_CREAT | O_DIRECT, 0666);
        if (fd == -1) {
            qLogFailfmt("Flusher: cannot open values file %s, error %s", VALUES_PATH.c_str(), strerror(errno));
            abort();
        }
        qLogDebugfmt("Flusher: File Descripter=[%d]", fd);
        while (1) {
            semop(flush_start, &sem_buf_down, 1);
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
                free(CommitQueue);
                free((void *) CommitCompletionQueue);
                free(InternalBuffer);
                int index_fd = open(INDECIES_PATH.c_str(), O_CREAT | O_TRUNC | O_RDWR | O_APPEND, 0666);
                GlobalIndexStore->persist(index_fd);
                for (int i = 0; i < HANDLER_THREADS; i++) {
                    std::cout << "--------------------ThreadId = " << i << "----------------" << std::endl;
                    PrintTiming(handtps[i]);
                    std::cout << "--------------------ThreadId = " << i << "----------------" << std::endl;
                }
                close(index_fd);
                close(fd);
                flock(lockfd, LOCK_UN);
                qLogSucc("Flusher unlocked filelock");
                exit(0);
            }
            if (internal_buffer_index == INTERNAL_BUFFER_LENGTH / VAL_SIZE) {
                internal_buffer_index = 0;
                semop(flush_end, &sem_buf_up, 1);
                qLogDebugfmt("Flusher: flush from %lu to %lu, with index %lu", INTERNAL_BUFFER_LENGTH / 2,
                             INTERNAL_BUFFER_LENGTH, internal_buffer_index);
                write(fd, InternalBuffer + (INTERNAL_BUFFER_LENGTH / 2),
                      INTERNAL_BUFFER_LENGTH / 2);
            } else {
                semop(flush_end, &sem_buf_up, 1);
                qLogDebugfmt("Flusher: flush from %lu to %lu, with index %lu", 0lu, INTERNAL_BUFFER_LENGTH / 2,
                             internal_buffer_index);
                write(fd, InternalBuffer, INTERNAL_BUFFER_LENGTH / 2);
            }
            WrittenIndex += INTERNAL_BUFFER_LENGTH / 2;
            qLogInfofmt("Flusher: written index = %lu", WrittenIndex);
        }
    }

}
