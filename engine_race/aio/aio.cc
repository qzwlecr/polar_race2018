#include "aio.h"
#include "../syscalls/sys.h"

namespace polar_race {

    AIOQuest::AIOQuest(uint16_t iotype, uint32_t srcfd, uintptr_t buffer, uint64_t nbytes, int64_t offset){
        cb = {0};
        cb.aio_data = buffer;
        cb.aio_lio_opcode = iotype;
        cb.aio_fildes = srcfd;
        cb.aio_buf = buffer;
        cb.aio_nbytes = nbytes;
        cb.aio_offset = offset;
        cb.aio_flags = 0;
    }

    AIOReadQuest::AIOReadQuest(uint32_t srcfd, uintptr_t buffer, uint64_t nbytes, int64_t offset):
    AIOQuest(IOCB_CMD_PREAD, srcfd, buffer, nbytes, offset){}

    AIOWriteQuest::AIOWriteQuest(uint32_t srcfd, uintptr_t buffer, uint64_t nbytes, int64_t offset):
    AIOQuest(IOCB_CMD_PWRITE, srcfd, buffer, nbytes, offset){}

    int AIOContext::setup(unsigned qdepth){
        return sys_aio_setup(qdepth, &ctx);
    }

    int AIOContext::submit(AIOQuest& quest){
        struct iocb * tmpptr = &(quest.cb);
        return sys_aio_submit(ctx, 1, &tmpptr);
    }

    int AIOContext::cancel(AIOQuest& quest, struct io_event* reslt){
        struct io_event ioev = {0};
        int rv = sys_aio_cancel(ctx, &(quest.cb), &ioev);
        if(reslt != NULL) *reslt = ioev;
        return rv;
    }

    int AIOContext::getevents(int64_t min_evs, int64_t max_evs, struct io_event* events, struct timespec* timeout){
        return sys_aio_getevents(ctx, min_evs, max_evs, events, timeout);
    }

    int AIOContext::destroy() {
        return sys_aio_destroy(ctx);
    }


};
