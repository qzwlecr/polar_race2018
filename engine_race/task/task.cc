#include "task.h"
#include "../format/log.h"
#include "../index/index.h"
#include "../consts/consts.h"

#include <iostream>

extern "C"{
#include <fcntl.h>
#include <unistd.h>
#include <sys/file.h>
#include <sys/types.h>
#include <malloc.h>
#include <sched.h>
}

namespace polar_race {

#define STRERR (strerror(errno))
#define LDOMAIN(x) ((x) + 1)

    volatile bool PreExitSign = false;
    volatile bool ExitSign = false;

    const uint32_t HB_MAGIC = 0x8088;

    char *InternalBuffer[HANDLER_THREADS];

    uint64_t AllocatedOffset[HANDLER_THREADS];

    Accumulator NextIndex(0);
    Accumulator TermCount(0);
    Accumulator InitCount(0);

    void SelfCloser(int timeout, bool *running) {
        int timed = 0;
        while (*running) {
            sleep(1);
            timed += 1;
            if (timed >= timeout) {
                qLogFail("SelfCloser: Sanity execution time exceeded.");
                qLogFail("SelfCloser: Forcibly termination..");
                exit(127);
            }
        }
        qLogSucc("SelfCloser: Exiting Gracefully..");
    }

    void HeartBeater(std::string sendaddr, bool *running) {
        qLogSuccfmt("HeartBeater: initialize %s", LDOMAIN(sendaddr.c_str()));
        MailBox hbmb;
        if (UNLIKELY(hbmb.open() == -1)) {
            qLogFailfmt("HeartBeat MailBox open failed: %s", strerror(errno));
            abort();
        }
        struct sockaddr_un un_sendaddr = mksockaddr_un(sendaddr);
        while (*running) {
            if (UNLIKELY(hbmb.sendOne(reinterpret_cast<const char *>(&HB_MAGIC),
                                      sizeof(HB_MAGIC), &un_sendaddr) == -1)) {
                qLogWarnfmt("HeartBeat Send GG?? is receiver GG?? %s", strerror(errno));
                /* abort(); */
            }
            qLogDebug("HeartBeater: beat!");
            sleep(1);
        }
        hbmb.close();
    }

    void BusyChecker(std::atomic<unsigned char> *atarray, uint8_t mode_busy, uint8_t mode_idle, bool *running) {
        qLogSuccfmt("BusyChecker: initialize %s", LDOMAIN(HANDLER_READY_ADDR.c_str()));
        MailBox hbcmb(HANDLER_READY_ADDR);
        if (UNLIKELY(hbcmb.desc == -1)) {
            qLogFailfmt("BusyChecker: MailBox open failed: %s", strerror(errno));
            abort();
        }
        struct sockaddr_un hbaddr = {0};
        Multiplexer mp;
        if (UNLIKELY(mp.open() == -1)) {
            qLogFailfmt("BusyChecker: Multiplexer open failed: %s", STRERR);
            abort();
        }
        if (UNLIKELY(mp.listen(hbcmb) == -1)) {
            qLogFailfmt("BusyChecker: Multiplexer listen HBMailBox failed: %s", STRERR);
            abort();
        }
        MailBox successer;
        uint8_t okid = 0;
        while (*running) {
            int rv = mp.wait(&successer, 1, 1000);
            if (UNLIKELY(rv == -1)) {
                qLogWarnfmt("BusyChecker: Multiplexer Wait Failed: %s", STRERR);
                continue;
            }
            if (UNLIKELY(rv == 0)) {
                continue;
            }
            // not very ok exactly..
            int rdv = hbcmb.getOne(reinterpret_cast<char *>(&okid), sizeof(uint8_t), &hbaddr);
            qLogDebugfmt("BusyChecker: %hhu ok!", okid);
            if (UNLIKELY(rdv == -1)) {
                qLogFailfmt("BusyChecker: unexpected MailBox Get Failure: %s", STRERR);
                continue;
            }
            // set
            for (int i = 0; i < UDS_CONGEST_AMPLIFIER; i++) {
                uint8_t busy_mark = mode_busy;
               atarray[((uint32_t) okid) + (i * HANDLER_THREADS)].compare_exchange_strong(busy_mark, mode_idle);
                qLogDebugfmt("BusyChecker: cleared busy state for %u", ((uint32_t) okid) + (i * HANDLER_THREADS));
            }
        }
        qLogSucc("BusyChecker: Exiting Gracefully..");
    }

    void HeartBeatChecker(std::string recvaddr) {
        // ENSURE
        PreExitSign = false;
        qLogSuccfmt("HeartBeatChecker: initialize %s", LDOMAIN(recvaddr.c_str()));
        MailBox hbcmb(recvaddr);
        if (UNLIKELY(hbcmb.desc == -1)) {
            qLogFailfmt("HeartBeatChecker MailBox open failed: %s", strerror(errno));
            abort();
        }
        struct sockaddr_un hbaddr = {0};
        Multiplexer mp;
        if (UNLIKELY(mp.open() == -1)) {
            qLogFailfmt("HeartBeatChecker Multiplexer open failed: %s", STRERR);
            abort();
        }
        if (UNLIKELY(mp.listen(hbcmb) == -1)) {
            qLogFailfmt("HeartBeatChecker Multiplexer listen HBMailBox failed: %s", STRERR);
            abort();
        }
        MailBox successer;
        uint32_t hbmagic = 0;
        while (true) {
            int rv = mp.wait(&successer, 1, 3000);
            if (UNLIKELY(rv == -1)) {
                qLogFailfmt("HeartBeatChecker Multiplexer Wait Failed: %s", STRERR);
                continue;
            }
            if (UNLIKELY(rv == 0)) {
                qLogFail("HeartBeatChecker: Timed out.");
                // timed out!
                PreExitSign = true;
                // do clean work
                mp.close();
                hbcmb.close();
                return;
            }
            // not very ok exactly..
            int rdv = hbcmb.getOne(reinterpret_cast<char *>(&hbmagic), sizeof(HB_MAGIC), &hbaddr);
            qLogDebug("HeartBeatChecker: beat!");
            if (UNLIKELY(rdv == -1)) {
                qLogFailfmt("HeartBeatChecker unexpected MailBox Get Failure: %s", STRERR);
                PreExitSign = true;
                mp.close();
                hbcmb.close();
                return;
            }
        }
    }

    void RequestProcessor(std::string recvaddr, TimingProfile *tp, uint8_t own_id) {
        cpu_set_t set;
        CPU_ZERO(&set);
        CPU_SET(own_id, &set);
        if (sched_setaffinity(0, sizeof(set), &set) == -1){
            qLogFailfmt("RequestProcessor sched set affinity failed: %s", STRERR);
            abort();
        }
        MailBox rdymb;
        if (UNLIKELY(rdymb.open() == -1)) {
            qLogFailfmt("RequestProcessor ready MailBox open failed: %s", STRERR);
            abort();
        }
        struct sockaddr_un un_sendaddr = mksockaddr_un(HANDLER_READY_ADDR);
        MailBox reqmb(recvaddr);
        if (UNLIKELY(reqmb.desc == -1)) {
            qLogFailfmt("RequestProcessor recv MailBox open failed: %s", STRERR);
            abort();
        }
        struct sockaddr_un cliun = {0};
        RequestResponse *rr = reinterpret_cast<RequestResponse *>(memalign(4096, sizeof(RequestResponse)));
        int valuesfd = ::open(VALUES_PATH.c_str(), O_NOATIME | O_RDONLY);
        if (valuesfd == -1) {
            qLogFailfmt("Cannot open values file %s, is it created already??", VALUES_PATH.c_str());
            abort();
        }
        if (posix_fadvise64(valuesfd, 0, 0, POSIX_FADV_RANDOM) == -1){
            qLogFailfmt("RequestProcessor: Cannot advise file usage pattern: %s", strerror(errno));
            abort();
        }
        int valuesfd_w = ::open(VALUES_PATH.c_str(), O_DSYNC | O_WRONLY | O_DIRECT);
        if (valuesfd_w == -1) {
            qLogFailfmt("Cannot open values file %s, is it created already??", VALUES_PATH.c_str());
            abort();
        }
        Multiplexer mp;
        if (UNLIKELY(mp.open() == -1)) {
            qLogFailfmt("BusyChecker: Multiplexer open failed: %s", STRERR);
            abort();
        }
        if (UNLIKELY(mp.listen(reqmb) == -1)) {
            qLogFailfmt("BusyChecker: Multiplexer listen HBMailBox failed: %s", STRERR);
            abort();
        }
        InitCount.fetch_add(1);
        MailBox successer;
        uint64_t buffer_index = 0;
        struct timespec t = {0};
        while (true) {
            StartTimer(&t);
            int rv = mp.wait(&successer, 1, 1000);
            if (UNLIKELY(rv == -1)) {
                qLogWarnfmt("RequestProcessor[%s]: Multiplexer Wait Failed: %s", LDOMAIN(recvaddr.c_str()), STRERR);
                continue;
            }
            if (UNLIKELY(rv == 0)) {
                if (PreExitSign) {
                    if (pwrite(valuesfd_w, InternalBuffer[own_id], INTERNAL_BUFFER_LENGTH, AllocatedOffset[own_id]) !=
                        INTERNAL_BUFFER_LENGTH) {
                        qLogFailfmt("RequestProcessor[%s]: Write fail: %s", LDOMAIN(recvaddr.c_str()), STRERR);
                        abort();
                    }
                    uint64_t emm = TermCount.fetch_add(1);
                    if (emm == HANDLER_THREADS - 1) {
                        for (int i = 0; i < HANDLER_THREADS; ++i)
                            free(InternalBuffer[i]);
                        int index_fd = open(INDECIES_PATH.c_str(), O_CREAT | O_TRUNC | O_RDWR, 0666);
                        GlobalIndexStore->persist(index_fd);
                        close(index_fd);
                        flock(lockfd, LOCK_UN);
                        qLogSucc("Flusher unlocked filelock");
                        //ExitSign = true;
                        PrintTiming(*tp);
                        exit(0);
                    }
                    PrintTiming(*tp);
                    return;
                }
                continue;
            }
            tp->epoll_wait += GetTimeElapsed(&t);
            StartTimer(&t);
            ssize_t gv = reqmb.getOne(reinterpret_cast<char *>(rr),
                                      sizeof(RequestResponse), &cliun);
            tp->uds_rd += GetTimeElapsed(&t);
            if (UNLIKELY(gv != sizeof(RequestResponse))) {
                qLogFailfmt("RequestProcessor[%s]: getRequest failed or incomplete: %s(%ld)", LDOMAIN(recvaddr.c_str()),
                            STRERR, gv);
                continue;
            }
            // simply ok..
            if (rr->type == RequestType::TYPE_RD) {
                uint64_t key = *reinterpret_cast<uint64_t *>(rr->key);
                uint64_t file_offset = 0;
                qLogInfofmt("RequestProcessor[%s]: RD %s !", LDOMAIN(recvaddr.c_str()),
                            KVArrayDump(rr->key, 2).c_str());
                // look up in global index store
                StartTimer(&t);
                if (!GlobalIndexStore->get(key, file_offset)) {
                    tp->index_get += GetTimeElapsed(&t);
                    qLogInfofmt("RequestProcessor[%s]: Key not found !", LDOMAIN(recvaddr.c_str()));
                    rr->type = RequestType::TYPE_EEXIST;
                } else {
                    tp->index_get += GetTimeElapsed(&t);
                    uint8_t *handler_id_p = reinterpret_cast<uint8_t *>(&file_offset);
                    uint8_t handler_id = *(handler_id_p + 7);
                    *(handler_id_p + 7) = 0;
                    qLogDebugfmt("RequestProcessor[%s]: Get index = %lu", LDOMAIN(recvaddr.c_str()), file_offset);
                    int64_t sub = (int64_t) file_offset - (int64_t) AllocatedOffset[handler_id];
                    if (LIKELY(sub > (int64_t) INTERNAL_BUFFER_LENGTH || sub < 0)) {
                        READ_ON_DISK:
                        StartTimer(&t);
                        ssize_t rdv = pread(valuesfd, rr->value, VAL_SIZE, file_offset);
                        uint64_t rdtel = GetTimeElapsed(&t);
                        tp->rddsk.accumulate((rdtel / 1000));
                        tp->read_disk += rdtel;
                        if (UNLIKELY(rdv != VAL_SIZE)) {
                            qLogWarnfmt(
                                    "RequestProcessor[%s]: read failed or incomplete: %s(%ld), treated as NOT FOUND.",
                                    LDOMAIN(recvaddr.c_str()), STRERR, rdv);
                            qLogWarnfmt(
                                    "RequestProcessor[%s]: this normally indicates filesystem content and in-memory index incoherency.",
                                    LDOMAIN(recvaddr.c_str()));
                            qLogWarnfmt("RequestProcessor[%s]: you should recheck the whole process carefully!!",
                                        LDOMAIN(recvaddr.c_str()));
                            rr->type = RequestType::TYPE_EEXIST;
                        } else {
                            // read OK.
                            // release the spyce!
                            qLogDebugfmt("RequestProcessor[%s]: Value found on DISK", LDOMAIN(recvaddr.c_str()));
                            qLogDebugfmt("RequestProcessor[%s]: Value read off disk: %s", LDOMAIN(recvaddr.c_str()),
                                         KVArrayDump(rr->value, 2).c_str());
                            rr->type = RequestType::TYPE_OK;
                        }
                    } else {
                        //found on internal buffer, get it
                        memcpy(rr->value, InternalBuffer[handler_id] + sub, VAL_SIZE);
                        qLogDebugfmt("RequestProcessor[%s]: rr.value %s !", LDOMAIN(recvaddr.c_str()),
                                     KVArrayDump(rr->value, 2).c_str());
                        sub = (int64_t) file_offset - (int64_t) AllocatedOffset[handler_id];
                        if (UNLIKELY(sub < 0 || sub > (int64_t) INTERNAL_BUFFER_LENGTH)) {
                            goto READ_ON_DISK;
                        }
                        qLogDebugfmt("RequestProcessor[%s]: Value found on InternalBuffer",
                                     LDOMAIN(recvaddr.c_str()));
                        rr->type = RequestType::TYPE_OK;
                    }
                }
                StartTimer(&t);
                ssize_t sv = reqmb.sendOne(reinterpret_cast<char *>(rr), sizeof(RequestResponse), &cliun);
                tp->uds_wr += GetTimeElapsed(&t);
                if (UNLIKELY(sv == -1)) {
                    qLogFailfmt("RequestProcessor[%s]: Send Response fail: %s", LDOMAIN(recvaddr.c_str()), STRERR);
                    abort();
                }
            } else {
                qLogDebugfmt("RequestProcessor[%s]: WR !", LDOMAIN(recvaddr.c_str()));
                qLogDebugfmt("RequestProcessor[%s]: K %hu => V %hu", LDOMAIN(recvaddr.c_str()),
                             *reinterpret_cast<uint16_t *>(rr->key), *reinterpret_cast<uint16_t *>(rr->value));
                uint64_t file_offset = buffer_index + AllocatedOffset[own_id];
                qLogDebugfmt("RequestProcessor[%s]: Put index = %lu", LDOMAIN(recvaddr.c_str()), file_offset);
                uint8_t *handler_id_p = reinterpret_cast<uint8_t *>(&file_offset);
                *(handler_id_p + 7) = own_id;
                memcpy(InternalBuffer[own_id] + buffer_index, rr->value, VAL_SIZE);
                GlobalIndexStore->put(*reinterpret_cast<uint64_t *>(rr->key), file_offset);
                qLogDebugfmt("RequestProcessor[%s]: WR file_offset %lu !", LDOMAIN(recvaddr.c_str()), file_offset);
                buffer_index += VAL_SIZE;
                if (buffer_index == INTERNAL_BUFFER_LENGTH) {
                    rr->type = RequestType::TYPE_BUSY;
                } else {
                    rr->type = RequestType::TYPE_OK;
                }
                qLogDebugfmt("RequestProcessor[%s]: Processing Complete.", LDOMAIN(recvaddr.c_str()));
                ssize_t sv = reqmb.sendOne(reinterpret_cast<char *>(rr), sizeof(RequestResponse), &cliun);
                if (sv == -1) {
                    qLogFailfmt("RequestProcessor[%s]: Send Response fail: %s", LDOMAIN(recvaddr.c_str()), STRERR);
                    abort();
                }
                if (buffer_index == INTERNAL_BUFFER_LENGTH) {
                    StartTimer(&t);
                    if (UNLIKELY(pwrite(valuesfd_w, InternalBuffer[own_id], INTERNAL_BUFFER_LENGTH,
                                        AllocatedOffset[own_id]) !=
                                 INTERNAL_BUFFER_LENGTH)) {
                        qLogFailfmt("RequestProcessor[%s]: Write fail: %s", LDOMAIN(recvaddr.c_str()), STRERR);
                        abort();
                    }
                    tp->write_disk += GetTimeElapsed(&t);
                    StartTimer(&t);
                    if (UNLIKELY(rdymb.sendOne(reinterpret_cast<const char *>(&own_id),
                                               sizeof(own_id), &un_sendaddr) == -1)) {
                        qLogFailfmt("RequestProcessor[%s]: Send Ready fail: %s", LDOMAIN(recvaddr.c_str()), STRERR);
                        abort();
                    }
                    tp->uds_wr += GetTimeElapsed(&t);
                    AllocatedOffset[own_id] = NextIndex.fetch_add(INTERNAL_BUFFER_LENGTH);
                    buffer_index = 0;
                }
            }

        }
    }

}

