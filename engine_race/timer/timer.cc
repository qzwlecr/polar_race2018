#include "timer.h"

#include <iostream>

using namespace std;

struct timespec begintm;

void StartTimer() {
    clock_gettime(CLOCK_MONOTONIC, &begintm);
}

void StartTimer(struct timespec *btm) {
    clock_gettime(CLOCK_MONOTONIC, btm);
}

void timespec_diff(struct timespec *start, struct timespec *stop,
                   struct timespec *result) {
    if ((stop->tv_nsec - start->tv_nsec) < 0) {
        result->tv_sec = stop->tv_sec - start->tv_sec - 1;
        result->tv_nsec = stop->tv_nsec - start->tv_nsec + 1000000000UL;
    } else {
        result->tv_sec = stop->tv_sec - start->tv_sec;
        result->tv_nsec = stop->tv_nsec - start->tv_nsec;
    }
}

uint64_t GetTimeElapsed() {
    struct timespec tmspc, reslt;
    clock_gettime(CLOCK_MONOTONIC, &tmspc);
    timespec_diff(&begintm, &tmspc, &reslt);
    return (uint64_t) reslt.tv_sec * 1000000000lu + reslt.tv_nsec;
}

uint64_t GetTimeElapsed(struct timespec *tx) {
    struct timespec tmspc, reslt;
    clock_gettime(CLOCK_MONOTONIC, &tmspc);
    timespec_diff(tx, &tmspc, &reslt);
    return (uint64_t) reslt.tv_sec * 1000000000lu + reslt.tv_nsec;
}

void TimingDistribution::accumulate(uint64_t eltime) {
    total++;
    if (eltime < 10) less10++;
    else if (eltime < 50) f10to50++;
    else if (eltime < 100) f50to100++;
    else if (eltime < 200) f100to200++;
    else if (eltime < 300) f200to300++;
    else large300++;
}

#define PCTG(x, tot) (((x) * 100 ) / (tot))

void TimingDistribution::print() const {
    printf("[  0, 10): %7u %3d%% \n", less10, PCTG(less10, total));
    printf("[ 10, 50): %7u %3d%% \n", f10to50, PCTG(f10to50, total));
    printf("[ 50,100): %7u %3d%% \n", f50to100, PCTG(f50to100, total));
    printf("[100,200): %7u %3d%% \n", f100to200, PCTG(f100to200, total));
    printf("[200,300): %7u %3d%% \n", f200to300, PCTG(f200to300, total));
    printf("[300,INF): %7u %3d%% \n", large300, PCTG(large300, total));
}

void PrintTiming(const TimingProfile &tp) {
    cout << "      Time Spent on:" << endl;
    cout << "          UDS Read: " << tp.uds_rd << endl;
    cout << "         UDS Write: " << tp.uds_wr << endl;
    cout << "       Indexer Put: " << tp.index_put << endl;
    cout << "       Indexer Get: " << tp.index_get << endl;
    cout << "       Spin Commit: " << tp.spin_commit << endl;
    cout << "         Read Disk: " << tp.read_disk << endl;
    cout << "      Read Disk Dist" << endl;
    tp.rddsk.print();
}
