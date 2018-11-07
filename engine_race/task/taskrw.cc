#include "task.h"
#include "../format/log.h"
#include "../index/index.h"
#include "../flusher/flusher.h"
#include "../consts/consts.h"

#include <iostream>
extern "C"{
#include <fcntl.h>
#include <unistd.h>
#include <malloc.h>
}

using namespace std;

namespace polar_race {

#define STRERR (strerror(errno))
#define LDOMAIN(x) ((x) + 1)

    const uint32_t HBRW_MAGIC = 0x8098;

    void HeartBeater_rw(string sendaddr, bool* rs){
        qLogSuccfmt("HeartBeaterRW: initialize %s", LDOMAIN(sendaddr.c_str()));
        MailBox hbmb;
        if (UNLIKELY(hbmb.open() == -1)) {
            qLogFailfmt("HeartBeaterRW: MailBox open failed: %s", strerror(errno));
            abort();
        }
        struct sockaddr_un un_sendaddr = mksockaddr_un(sendaddr);
        while (*rs) {
            if (UNLIKELY(hbmb.sendOne(reinterpret_cast<const char *>(&HBRW_MAGIC),
                                      sizeof(HBRW_MAGIC), &un_sendaddr) == -1)) {
                qLogWarnfmt("HeartBeaterRW: Send failed: %s", strerror(errno));
                /* abort(); */
            }
            qLogDebug("HeartBeaterRW: beat!");
            sleep(1);
        }
        hbmb.close();
    }

    void HeartBeatChecker_rw(string recvaddr){
        ExitSign = false;
        qLogSuccfmt("HeartBeatCheckerRW: initialize %s", LDOMAIN(recvaddr.c_str()));
        MailBox hbcmb(recvaddr);
        if (UNLIKELY(hbcmb.desc == -1)) {
            qLogFailfmt("HeartBeatCheckerRW: MailBox open failed: %s", strerror(errno));
            abort();
        }
        struct sockaddr_un hbaddr = {0};
        Multiplexer mp;
        if (UNLIKELY(mp.open() == -1)) {
            qLogFailfmt("HeartBeatCheckerRW: Multiplexer open failed: %s", STRERR);
            abort();
        }
        if (UNLIKELY(mp.listen(hbcmb) == -1)) {
            qLogFailfmt("HeartBeatCheckerRW: Multiplexer listen HBMailBox failed: %s", STRERR);
            abort();
        }
        MailBox successer;
        uint32_t hbmagic = 0;
        while (true) {
            int rv = mp.wait(&successer, 1, 3000);
            if (UNLIKELY(rv == -1)) {
                qLogFailfmt("HeartBeatCheckerRW: Multiplexer Wait Failed: %s", STRERR);
                continue;
            }
            if (UNLIKELY(rv == 0)) {
                qLogFail("HeartBeatCheckerRW: Timed out. --> Terminating");
                // timed out!
                ExitSign = true;
                // do clean work
                mp.close();
                hbcmb.close();
                // TODO: Persist Index
                exit(0);
            }
            // not very ok exactly..
            int rdv = hbcmb.getOne(reinterpret_cast<char *>(&hbmagic), sizeof(HBRW_MAGIC), &hbaddr);
            qLogDebug("HeartBeatCheckerRW: beat!");
            if (UNLIKELY(rdv == -1)) {
                qLogFailfmt("HeartBeatCheckerRW: unexpected MailBox Get Failure: %s", STRERR);
                ExitSign = true;
                mp.close();
                hbcmb.close();
                // TODO: Persist Index
                exit(0);
            }
        }
    }

    void RequestPorcessor_rw(string recvaddr, TimingProfile* pf){

    }


};
