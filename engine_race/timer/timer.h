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

#define GET_TIME_ELAPSED

void StartTimer__(struct timespec* t);
#ifdef GET_TIME_ELAPSED
#define StartTimer(x) StartTimer__(x)
#else
#define StartTimer(x) ;
#endif

uint64_t GetTimeElapsed__(struct timespec* t);
#ifdef GET_TIME_ELAPSED
#define GetTimeElapsed(x) GetTimeElapsed__(x)
#else
#define GetTimeElapsed(x) (0)
#endif

void PrintTiming(const TimingProfile& tp);

#endif
