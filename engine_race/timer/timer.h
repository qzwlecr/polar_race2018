// PerformanceTimer.h - High Resolution Timer
// Since not every PC has performance hardware, we have fallback mechanism
// to timeGetTime()
//

#ifndef __PERFORMANCETIMER_H__
#define __PERFORMANCETIMER_H__

#include <cstdint>
#include <ctime>

struct TimingProfile {
    uint64_t uds_rd;
    uint64_t uds_wr;
    uint64_t index_put;
    uint64_t index_get;
    uint64_t spin_commit;
    uint64_t read_disk;
};

void StartTimer();
void StartTimer(struct timespec* t);
uint64_t GetTimeElapsed();
uint64_t GetTimeElapsed(struct timespec* t);

void PrintTiming(const TimingProfile& tp);

#endif
