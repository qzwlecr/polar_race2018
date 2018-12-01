#ifndef POLARDB_AIO_H
#define POLARDB_AIO_H

#include "../syscalls/sys.h"
#include <cstdint>

namespace polar_race {

    class AIOQuest {
        protected:
            struct iocb;
        public:
            AIOQuest(uint16_t iotype, uint32_t srcfd, uintptr_t buffer, uint64_t nbytes, int64_t offset);
    };

    class AIOReadQuest: public AIOQuest{
        public:
            AIOReadQuest(uint32_t srcfd, uintptr_t buffer, uint64_t nbytes, int64_t offset);
    };

    class AIOWriteQuest: public AIOQuest{
        public:
            AIOWriteQuest(uint32_t srcfd, uintptr_t buffer, uint64_t nbytes, int64_t offset);
    };

    class AIOContext {
        private:
            aio_context_t ctx;
        public:
            int setup(unsigned qdepth);
            int submit(const AIOQuest& quest);
            int cancel(const AIOQuest& quest, struct io_event* reslt);
            int getevents(int64_t min_evs, int64_t max_evs, struct io_event* events, struct timespec* timeout);
            int destroy();
    };

};

#endif
