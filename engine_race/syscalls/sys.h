#ifndef RALOPDB_SYSCALLS_H
#define RALOPDB_SYSCALLS_H

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

extern "C" {
#include <sched.h>
};

namespace polar_race{

int sys_getaff(pid_t pid, int* cpuid);
int sys_setaff(pid_t pid, int cpuid);

};


#endif
