#include "task.h"
#include "../format/log.h"
#include "../index/index.h"
#include "../consts/consts.h"

#include <iostream>
extern "C"{
#include <fcntl.h>
#include <unistd.h>
#include <malloc.h>
#include <sys/file.h>
}

using namespace std;

namespace polar_race {

#define STRERR (strerror(errno))
#define LDOMAIN(x) ((x) + 1)
#define unlikely(x) UNLIKELY(x)
#define likely(x) likely(x)

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
                // do clean work
                mp.close();
                hbcmb.close();
                int index_fd = open(INDECIES_PATH.c_str(), O_CREAT | O_TRUNC | O_RDWR | O_APPEND, 0666);
                if(index_fd == -1){
                    qLogFailfmt("HeartBeatCheckerRW: Cannot open Index file %s to persist: %s",
                            INDECIES_PATH.c_str(), STRERR);
                } else {
                    GlobalIndexStore->persist(index_fd);
                }
                flock(lockfd, LOCK_UN);
                qLogSucc("HeartBeatCheckerRW: Unlocked filelock");
                close(index_fd);
                exit(0);
            }
            // not very ok exactly..
            int rdv = hbcmb.getOne(reinterpret_cast<char *>(&hbmagic), sizeof(HBRW_MAGIC), &hbaddr);
            qLogDebug("HeartBeatCheckerRW: beat!");
            if (UNLIKELY(rdv == -1)) {
                qLogFailfmt("HeartBeatCheckerRW: unexpected MailBox Get Failure: %s", STRERR);
                mp.close();
                hbcmb.close();
                int index_fd = open(INDECIES_PATH.c_str(), O_CREAT | O_TRUNC | O_RDWR | O_APPEND, 0666);
                if(index_fd == -1){
                    qLogFailfmt("HeartBeatCheckerRW: Cannot open Index file %s to persist: %s",
                            INDECIES_PATH.c_str(), STRERR);
                } else {
                    GlobalIndexStore->persist(index_fd);
                }
                flock(lockfd, LOCK_UN);
                qLogSucc("HeartBeatCheckerRW: Unlocked filelock");
                close(index_fd);
                exit(0);
            }
        }
    }

    void RequestProcessor_rw(string recvaddr, TimingProfile* pf){
        // the key is we only do index works, leave the IO to caller itself
        // to prevent excessive memory copy
        MailBox reqmb(recvaddr);
        if (UNLIKELY(reqmb.desc == -1)) {
            qLogFailfmt("RequestProcessorRW[%s]: recv MailBox open failed: %s", LDOMAIN(recvaddr.c_str()), STRERR);
            abort();
        }
        struct sockaddr_un cliun = {0};
        RequestResponseRW rr = {0};
        struct timespec t = {0};
        while(true){
            StartTimer(&t);
            ssize_t rv = reqmb.getOne(reinterpret_cast<char*>(&rr),
                    sizeof(RequestResponseRW), &cliun);
            pf->uds_rd += GetTimeElapsed(&t);
            if(unlikely(rv != sizeof(RequestResponseRW))) {
                qLogFailfmt("RequestProcessorRW[%s]: getRequest failed or incomplete: %s(%ld)",
                        LDOMAIN(recvaddr.c_str()), STRERR, rv);
                continue;
            }
            // Recv OK
            if(rr.type == RequestTypeRW::TYPERW_GET){
                uint64_t key = *reinterpret_cast<uint64_t *>(rr.key);
                uint64_t file_offset = 0;
                StartTimer(&t);
                if (!GlobalIndexStore->get(key, file_offset)) {
                    pf->index_get += GetTimeElapsed(&t);
                    // not found
                    qLogDebugfmt("RequestProcessorRW[%s]: Key %s not found !",
                            LDOMAIN(recvaddr.c_str()), KVArrayDump(rr.key, KEY_SIZE).c_str());
                    rr.type = RequestTypeRW::TYPERW_EEXIST;
                    StartTimer(&t);
                    int sv = reqmb.sendOne(reinterpret_cast<char *>(&rr), sizeof(RequestResponseRW), &cliun);
                    pf->uds_wr += GetTimeElapsed(&t);
                    if (sv == -1) {
                        qLogFailfmt("ReqeustProcessorRW[%s]: Send Response fail: %s", LDOMAIN(recvaddr.c_str()), STRERR);
                        abort();
                    }
                } else {
                    pf->index_get += GetTimeElapsed(&t);
                    qLogDebugfmt("RequestProcessorRW[%s]: Get %s => %lu",
                            LDOMAIN(recvaddr.c_str()), KVArrayDump(rr.key, KEY_SIZE).c_str(), file_offset);
                    rr.foffset = file_offset;
                    rr.type = RequestTypeRW::TYPERW_OK;
                    StartTimer(&t);
                    int sv = reqmb.sendOne(reinterpret_cast<char *>(&rr), sizeof(RequestResponseRW), &cliun);
                    pf->uds_wr += GetTimeElapsed(&t);
                    if (sv == -1) {
                        qLogFailfmt("ReqeustProcessorRW[%s]: Send Response fail: %s", LDOMAIN(recvaddr.c_str()), STRERR);
                        abort();
                    }
                }
            } else if(rr.type == RequestTypeRW::TYPERW_PUT){
                qLogDebugfmt("RequestProcessorRW[%s]: Put %s => %lu",
                        LDOMAIN(recvaddr.c_str()), KVArrayDump(rr.key, KEY_SIZE).c_str(), rr.foffset);
                StartTimer(&t);
                GlobalIndexStore->put(*reinterpret_cast<uint64_t*>(rr.key),rr.foffset);
                pf->index_put += GetTimeElapsed(&t);
                rr.type = RequestTypeRW::TYPERW_OK;
                StartTimer(&t);
                int sv = reqmb.sendOne(reinterpret_cast<char *>(&rr), sizeof(RequestResponseRW), &cliun);
                pf->uds_wr += GetTimeElapsed(&t);
                if (sv == -1) {
                    qLogFailfmt("ReqeustProcessorRW[%s]: Send Response fail: %s", LDOMAIN(recvaddr.c_str()), STRERR);
                    abort();
                }
            } else {
                // nor GET nor PUT
                // what are you?
                qLogFailfmt("RequestProcessorRW[%s]: Invalid RequestRW type %hhu",
                        LDOMAIN(recvaddr.c_str()), rr.type);
                qLogFailfmt("RequestProcessorRW[%s]: This normally indicate an implementation error.",
                        LDOMAIN(recvaddr.c_str()));
                abort();
            }
        }
    }


};
