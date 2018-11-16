// PerformanceTimer.h - High Resolution Timer
// Since not every PC has performance hardware, we have fallback mechanism
// to timeGetTime()
//

#ifndef __PERFORMANCETIMER_H__
#define __PERFORMANCETIMER_H__

#include <cstdint>
#include <ctime>

struct TimingDistribution {
    uint32_t less10;
    uint32_t f10to50;
    uint32_t f50to100;
    uint32_t f100to200;
    uint32_t f200to300;
    uint32_t large300;
    uint32_t total;
    TimingDistribution():
        less10(0), f10to50(0), f50to100(0),
        f100to200(0), f200to300(0), large300(0),
        total(1) {};
    void accumulate(uint64_t eltime);
    void print()const;
};

struct TimingProfile {
    uint64_t uds_rd;
    uint64_t uds_wr;
    uint64_t epoll_wait;
    uint64_t index_put;
    uint64_t index_get;
    uint64_t write_disk;
    uint64_t read_disk;
    TimingDistribution rddsk;
};

#define MICROSEC(x) ((x)/1000)

void StartTimer();
void StartTimer(struct timespec* t);
uint64_t GetTimeElapsed();
uint64_t GetTimeElapsed(struct timespec* t);

void PrintTiming(const TimingProfile& tp);

#endif
