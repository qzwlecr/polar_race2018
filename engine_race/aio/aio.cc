#include "aio.h"
#include "../syscalls/sys.h"

namespace polar_race {

    AIOQuest::AIOQuest(uint16_t iotype, uint32_t srcfd, uintptr_t buffer, uint64_t nbytes, int64_t offset){
        // TODO: implement
    }

    AIOReadQuest::AIOReadQuest(uint32_t srcfd, uintptr_t buffer, uint64_t nbytes, int64_t offset):
    AIOQuest(IOCB_CMD_PREAD, srcfd, buffer, nbytes, offset){}

    AIOWriteQuest::AIOWriteQuest(uint32_t srcfd, uintptr_t buffer, uint64_t nbytes, int64_t offset):
    AIOQuest(IOCB_CMD_PWRITE, srcfd, buffer, nbytes, offset){}

    int AIOContext::setup(unsigned qdepth){
        return sys_aio_setup(qdepth, &ctx);
    }

    int AIOContext::submit(const AIOQuest& quest){

    }

    int AIOContext::cancel(const AIOQuest& quest, struct io_event* reslt){

    }

    int AIOContext::getevents(int64_t min_evs, int64_t max_evs, struct io_event* events, struct timespec* timeout){

    }

    int AIOContext::destroy() {

    }


};
