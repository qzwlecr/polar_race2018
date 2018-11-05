// PerformanceTimer.h - High Resolution Timer
// Since not every PC has performance hardware, we have fallback mechanism
// to timeGetTime()
//

#ifndef __PERFORMANCETIMER_H__
#define __PERFORMANCETIMER_H__

#include <cstdint>
#include <ctime>

void StartTimer();
void StartTimer(struct timespec* t);
uint64_t GetTimeElapsed();
uint64_t GetTimeElapsed(struct timespec* t);

#endif
