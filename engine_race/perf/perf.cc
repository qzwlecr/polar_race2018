#include "perf.h"
#include "../commu/commu.h" // for Accumulator
#include "../format/log.h"
#include "../timer/timer.h"

#include <thread>
#include <vector>

extern "C" {
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
}

using namespace std;

namespace polar_race {
    Accumulator totalTm(0);
    void BenchMain(string prefix){
        // TODO: write your benchmarks!!
        // bench_run to run single thread bench and automatically timed, while
        // bench_concurrent_run will time overall executions of multiple threads!!
        // in your executor/thexecutor functions, you can use
        // bench_read / bench_write to perform read and write with meaningless data
        // to some offset with some size.
        // TODO
        

        //////////////////////////
        // make sure the bench server complain about our program.
        // dont mess this up, this should always be the last thing the 
        // BenchMain executed.
        abort();
    }


    uint64_t bench_run(void (*executor)(int), int fd){
        struct timespec t = {0};
        StartTimer(&t);
        executor(fd);
        return GetTimeElapsed(&t);
    }
    void bench_concurrent_worker(void (*thexecutor)(int, int), int fd, int myid){
        struct timespec t = {0};
        StartTimer(&t);
        thexecutor(fd, myid);
        totalTm.fetch_add(GetTimeElapsed(&t));
    }
    uint64_t bench_concurrent_run(void (*thexecutor)(int myid, int fd), int fd, int concurrency){
        vector<thread*> thrds;
        for(int i = 0; i < concurrency; i++){
            thrds.push_back(new thread(bench_concurrent_worker, i, fd));
        }
        for(auto &x: thrds){
            x->join();
            delete x;
        }
        uint64_t tt = totalTm;
        totalTm = 0;
        return tt;
    }
    void bench_read(int fd, off_t offset, size_t sz){
        char fakebuf[sz];
        pread(fd, fakebuf, sz, offset);
    }
    void bench_write(int fd, off_t offset, size_t sz){
        char nocont[sz];
        pwrite(fd, nocont, sz, offset);
    }
};
