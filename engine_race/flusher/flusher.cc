//
// Created by qzwlecr on 10/31/18.
//

#include <malloc.h>
#include "flusher/flusher.h"
#include "index/index.h"

namespace polar_race {
    using namespace std;
    char *CommitQueue;
    std::atomic<uint8_t> *CommitCompletionQueue;
    volatile uint64_t WrittenIndex = 0;
    char *InternalBuffer;

    Flusher::Flusher() : flushing_index(0) {
        size_t pagesize = (size_t) getpagesize();
        CommitQueue = (char *) memalign(pagesize, COMMIT_QUEUE_LENGTH * VAL_SIZE);
        CommitCompletionQueue = (atomic<uint8_t> *) memalign(pagesize, COMMIT_QUEUE_LENGTH * sizeof(atomic<uint8_t>));
        InternalBuffer = (char *) memalign(pagesize, INTERNAL_BUFFER_LENGTH);
        if (CommitQueue == nullptr || CommitCompletionQueue == nullptr || InternalBuffer == nullptr) {
            qLogFailfmt("Flusher: allocating memory error %s", strerror(errno));
            abort();
        }
        memset(CommitQueue, 0, COMMIT_QUEUE_LENGTH * VAL_SIZE);
        for (uint64_t i = 0; i< COMMIT_QUEUE_LENGTH; ++i){
            CommitCompletionQueue[i] = 0;
        }
        memset(InternalBuffer, 0, INTERNAL_BUFFER_LENGTH);

    }

    void Flusher::flush_begin() {
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
            while (CommitCompletionQueue[index].load() != COMMIT_COMPLETION_FULL) {
                if (UNLIKELY(ExitSign)) {
                    last_flush = true;
                    return nullptr;
                }
            }
            if (internal_buffer_index == (INTERNAL_BUFFER_LENGTH / 2 / VAL_SIZE) ||
                internal_buffer_index == INTERNAL_BUFFER_LENGTH / VAL_SIZE) {
                uint64_t new_index = internal_buffer_index / (INTERNAL_BUFFER_LENGTH / 2 / VAL_SIZE), expected;
                do {
                    expected = 0;
                }while(flushing_index.compare_exchange_weak(expected, new_index) == false);
                qLogDebugfmt("Reader: flushing index = %d", flushing_index.load());
                qLogDebug("Reader: Ready to flush to disk");
                if(internal_buffer_index == (INTERNAL_BUFFER_LENGTH / VAL_SIZE)){
                    internal_buffer_index = 0;
                }
            }
            memcpy(InternalBuffer + internal_buffer_index * VAL_SIZE,
                   CommitQueue + index * VAL_SIZE,
                   VAL_SIZE);
            internal_buffer_index++;
            CommitCompletionQueue[index].store(COMMIT_COMPLETION_EMPTY);
            qLogDebugfmt("Reader: %lu", internal_buffer_index);
        }

    }

    void *Flusher::flush() {
        int fd = open(VALUES_PATH.c_str(), O_RDWR | O_APPEND | O_DSYNC | O_CREAT | O_DIRECT, 0666);
        if (fd == -1) {
            qLogFailfmt("Flusher: cannot open values file %s, error %s", VALUES_PATH.c_str(), strerror(errno));
            abort();
        }
        qLogDebugfmt("Flusher: File Descripter=[%d]", fd);
        while (1) {
            while (flushing_index.load()==0 && UNLIKELY(!ExitSign));
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
                free(CommitCompletionQueue);
                free(InternalBuffer);
                int index_fd = open(INDECIES_PATH.c_str(), O_CREAT | O_TRUNC | O_RDWR | O_APPEND, 0666);
                GlobalIndexStore->persist(index_fd);
                for (int i = 0; i < HANDLER_THREADS; i++) {
                    cout << "Handler " << i << endl;
                    PrintTiming(handtps[i]);
                }
                close(index_fd);
                close(fd);
                flock(lockfd, LOCK_UN);
                qLogSucc("Flusher unlocked filelock");
                exit(0);
            }
            write(fd, InternalBuffer + (flushing_index - 1)*(INTERNAL_BUFFER_LENGTH / 2),
                  INTERNAL_BUFFER_LENGTH / 2);
            WrittenIndex += INTERNAL_BUFFER_LENGTH / 2;
            flushing_index.store(0);
            qLogDebugfmt("Flusher: written index = %lu", WrittenIndex);
        }
    }

}
