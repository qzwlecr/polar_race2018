#ifndef ENGINE_RACE_TASK_H
#define ENGINE_RACE_TASK_H

#include <cstdint>
#include <string>
#include "../commu/commu.h"
#include "../timer/timer.h"

namespace polar_race {

    extern volatile int lockfd;

    extern volatile bool ExitSign; // on true, execute exit clean proc

    extern Accumulator NextIndex;

    const std::string HB_ADDR = (std::string() + '\0') + "ralopdb/heartbeat";
    const std::string REQ_ADDR_PREFIX = (std::string() + '\0') + "ralopdb/request/";
    const std::string RESP_ADDR_PREFIX = (std::string() + '\0') + "ralopdb/respond/";
    const std::string HANDLER_READY_ADDR = (std::string() + '\0') + "ralopdb/ready";

    // MODE_MPROC_SEQ_WR
    void RequestProcessor(std::string recvaddr, TimingProfile* tmpf);

    void HeartBeater(std::string sendaddr, bool *runstate);

    void HeartBeatChecker(std::string recvaddr);

    // MODE_MPROC_RAND_WR
    void RequestProcessor_rw(std::string recvaddr, TimingProfile* tmpf);
    void HeartBeater_rw(std::string sendaddr, bool* runstate);
    void HeartBeatChecker_rw(std::string recvaddr);

    // Universal Tools
    void SelfCloser(int timeout, bool* running);
};

#endif
