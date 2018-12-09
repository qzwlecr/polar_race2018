#ifndef ENGINE_RACE_TASK_H
#define ENGINE_RACE_TASK_H

#include <cstdint>
#include <string>
#include "../commu/commu.h"
#include "../timer/timer.h"
#include "aio/aio.h"

namespace polar_race {

    extern volatile int lockfd;

    extern volatile bool ExitSign; // on true, execute exit clean proc

    const std::string HB_ADDR = (std::string() + '\0') + "ralopdb/heartbeat";
    const std::string REQ_ADDR_PREFIX = (std::string() + '\0') + "ralopdb/request/";
    const std::string RESP_ADDR_PREFIX = (std::string() + '\0') + "ralopdb/respond/";

    // MODE_MPROC_SEQ_WR
    void RequestProcessor(std::string recvaddr, TimingProfile* tmpf, uint8_t own_id);

    void HeartBeater(std::string sendaddr, bool *runstate);

    void HeartBeatChecker(std::string recvaddr);

    void AIOQueueCleaner(AIOContext* ctx, void (*cleanop)(uint64_t aiodata), bool* running);

    // Universal Tools
    void SelfCloser(int timeout, bool* running);
};

#endif
