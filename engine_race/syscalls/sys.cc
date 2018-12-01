#include "sys.h"
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
extern "C"{
#include <unistd.h>
#include <sys/syscall.h>
}

namespace polar_race {

int sys_getaff(pid_t pid, int* cpuid){
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    int rv = sched_getaffinity(pid, sizeof(cpu_set_t), &cpuset);
    if(rv == -1 || cpuid == nullptr) return rv;
    if(CPU_COUNT(&cpuset) != 1) return rv;
    for(int i = 0; i < *cpuid; i++){
        if(CPU_ISSET(i, &cpuset)){
            *cpuid = i;
            return rv;
        }
    }
    return rv;
}

int sys_setaff(pid_t pid, int cpuid){
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(cpuid, &cpuset);
    return sched_setaffinity(pid, sizeof(cpu_set_t), &cpuset);
}

int sys_aio_setup(unsigned nr_evs, aio_context_t* ctx){
    return syscall(SYS_io_setup, nr_evs, ctx);
}

int sys_aio_destroy(aio_context_t ctx){
    return syscall(SYS_io_destroy, ctx);
}

int sys_aio_submit(aio_context_t ctx, long nreqs, struct iocb **reqarr){
    return syscall(SYS_io_submit, ctx, nreqs, reqarr);
}

int sys_aio_cancel(aio_context_t ctx, struct iocb* req, struct io_event* result){
    return syscall(SYS_io_cancel, ctx, req, result);
}

int sys_aio_getevents(aio_context_t ctx, long min_nevs, long max_nevs, struct io_event* evs, struct timespec* timeout){
    return syscall(SYS_io_getevents, ctx, min_nevs, max_nevs, evs, timeout);
}

};
