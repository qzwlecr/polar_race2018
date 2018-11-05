#include "timer.h"

#include <stdint.h>

struct timespec begintm;

void StartTimer(){
	clock_gettime(CLOCK_MONOTONIC, &begintm);
}

void StartTimer(struct timespec* btm){
	clock_gettime(CLOCK_MONOTONIC, btm);
}

void timespec_diff(struct timespec *start, struct timespec *stop,
                   struct timespec *result){
    if ((stop->tv_nsec - start->tv_nsec) < 0) {
        result->tv_sec = stop->tv_sec - start->tv_sec - 1;
        result->tv_nsec = stop->tv_nsec - start->tv_nsec + 1000000000UL;
    } else {
        result->tv_sec = stop->tv_sec - start->tv_sec;
        result->tv_nsec = stop->tv_nsec - start->tv_nsec;
    }
}

uint64_t GetTimeElapsed(){
	struct timespec tmspc,reslt;
	clock_gettime(CLOCK_MONOTONIC,&tmspc);
	timespec_diff(&begintm, &tmspc, &reslt);
	return (uint64_t)reslt.tv_sec*1000000000lu+ reslt.tv_nsec;
}

uint64_t GetTimeElapsed(struct timespec* tx){
	struct timespec tmspc,reslt;
	clock_gettime(CLOCK_MONOTONIC,&tmspc);
	timespec_diff(tx, &tmspc, &reslt);
	return (uint64_t)reslt.tv_sec*1000000000lu+ reslt.tv_nsec;
}

