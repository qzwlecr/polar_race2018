#include "sys.h"

namespace polar_race {

int sys_getaff(pid_t pid, int* cpuid){
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    int rv = sched_getaffinity(pid, sizeof(cpu_set_t), &cpuset);
    if(rv == -1 || cpuid == NULL) return rv;
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

};
