#ifndef RALOPDB_SYSCALLS_H
#define RALOPDB_SYSCALLS_H

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

extern "C" {
#include <sched.h>
#include <linux/aio_abi.h>
#include <linux/time.h>
};

namespace polar_race{

int sys_getaff(pid_t pid, int* cpuid);
int sys_setaff(pid_t pid, int cpuid);

int sys_aio_setup(unsigned nr_evs, aio_context_t* ctx);
int sys_aio_destroy(aio_context_t ctx);
int sys_aio_submit(aio_context_t ctx, long nreqs, struct iocb **reqarr);
int sys_aio_cancel(aio_context_t ctx, struct iocb* req, struct io_event* result);
int sys_aio_getevents(aio_context_t ctx, long min_nevs, long max_nevs, struct io_event* evs, struct timespec* timeout);

};


#endif
