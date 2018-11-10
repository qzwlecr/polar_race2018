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

namespace polar_race {

#define STRERR (strerror(errno))
#define LDOMAIN(x) ((x) + 1)
    
    volatile bool ExitSign = false;

    const uint32_t HB_MAGIC = 0x8088;

    Accumulator NextIndex(0);

    void SelfCloser(int timeout, bool* running){
        int timed = 0;
        while(*running){
            sleep(1);
            timed += 1;
            if(timed >= timeout){
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

    void BusyChecker(std::atomic_uint8_t* atarray, uint8_t mode_busy, uint8_t mode_idle, bool* running){
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
            for(int i = 0; i < UDS_CONGEST_AMPLIFIER; i++){
                uint8_t busy_mark = mode_busy, idle_mark = mode_idle;
                if(atarray[((uint32_t)okid) + (i * HANDLER_THREADS)].compare_exchange_weak(busy_mark, idle_mark)){
                    qLogDebugfmt("BusyChecker: cleared busy state for %u", ((uint32_t)okid) + (i * HANDLER_THREADS));
                }
            }
        }
    }

    void HeartBeatChecker(std::string recvaddr) {
        // ENSURE
        ExitSign = false;
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
                ExitSign = true;
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
                ExitSign = true;
                mp.close();
                hbcmb.close();
                return;
            }
        }
    }

#define LARRAY_ACCESS(larr, offset, wrap) ((larr) + ((offset) % (wrap)))

    void RequestProcessor(std::string recvaddr, TimingProfile *tp) {
        MailBox reqmb(recvaddr);
        if (UNLIKELY(reqmb.desc == -1)) {
            qLogFailfmt("RequestProcessor recv MailBox open failed: %s", STRERR);
            abort();
        }
        struct sockaddr_un cliun = {0};
        RequestResponse* rr = reinterpret_cast<RequestResponse*>(memalign(4096, sizeof(RequestResponse)));
        int valuesfd = ::open(VALUES_PATH.c_str(), O_NOATIME);
        if (valuesfd == -1) {
            qLogFailfmt("Cannot open values file %s, is it created already??", VALUES_PATH.c_str());
            abort();
        }
        struct timespec t = {0};
        while (true) {
            StartTimer(&t);
            ssize_t gv = reqmb.getOne(reinterpret_cast<char *>(rr),
                                  sizeof(RequestResponse), &cliun);
            tp->uds_rd += GetTimeElapsed(&t);
            if (UNLIKELY(gv != sizeof(RequestResponse))) {
                qLogFailfmt("RequestProcessor[%s]: getRequest failed or incomplete: %s(%ld)", LDOMAIN(recvaddr.c_str()), STRERR, gv);
                continue;
            }
            // simply ok..
            if (rr->type == RequestType::TYPE_RD) {
                uint64_t key = *reinterpret_cast<uint64_t *>(rr->key);
                uint64_t file_offset = 0;
                qLogInfofmt("RequestProcessor[%s]: RD %s !", LDOMAIN(recvaddr.c_str()), KVArrayDump(rr->key, 2).c_str());
                // look up in global index store
                StartTimer(&t);
                if (!GlobalIndexStore->get(key, file_offset)) {
                    tp->index_get += GetTimeElapsed(&t);
                    // not found
                    qLogInfofmt("RequestProcessor[%s]: Key not found !", LDOMAIN(recvaddr.c_str()));
                    rr->type = RequestType::TYPE_EEXIST;
                    StartTimer(&t);
                    int sv = reqmb.sendOne(reinterpret_cast<char *>(rr), sizeof(RequestResponse), &cliun);
                    tp->uds_wr += GetTimeElapsed(&t);
                    if (sv == -1) {
                        qLogFailfmt("ReqeustProcessor[%s]: Send Response fail: %s", LDOMAIN(recvaddr.c_str()), STRERR);
                        abort();
                    }
                } else {
                    tp->index_get += GetTimeElapsed(&t);
                    qLogInfofmt("RequestProcessor[%s]: file_offset %lu, WrittenIdx %lu !", LDOMAIN(recvaddr.c_str()),
                                file_offset, WrittenIndex);
                    // check WrittenIndex against expectedIndex
                    if (file_offset >= WrittenIndex) {
                        // read from internal buffer
                        memcpy(rr->value, LARRAY_ACCESS(InternalBuffer, file_offset,INTERNAL_BUFFER_LENGTH), VAL_SIZE);
                        qLogDebugfmt("RequestProcessor[%s]: rr.value %s !", LDOMAIN(recvaddr.c_str()), KVArrayDump(rr->value, 2).c_str());
                        // check WrittenIndex again
                        if (file_offset >= WrittenIndex) {
                            // then we should return it
                            qLogDebugfmt("RequestProcessor[%s]: Value found on InternalBuffer",
                                        LDOMAIN(recvaddr.c_str()));
                            rr->type = RequestType::TYPE_OK;
                            StartTimer(&t);
                            int sv = reqmb.sendOne(reinterpret_cast<char *>(rr), sizeof(RequestResponse), &cliun);
                            tp->uds_wr += GetTimeElapsed(&t);
                            if (sv == -1) {
                                qLogFailfmt("ReqeustProcessor[%s]: Send Response fail: %s", LDOMAIN(recvaddr.c_str()),
                                            STRERR);
                                abort();
                            }
                            qLogDebugfmt("RequestProcessor[%s]: Processing Complete.", LDOMAIN(recvaddr.c_str()));
                            continue;
                        }
                    }
                    // that means we should read it from file
                    int seekv = lseek(valuesfd, file_offset, SEEK_SET);
                    if (seekv == -1) {
                        qLogWarnfmt("RequestProcessor[%s]: lseek failed: %s, treated as NOT FOUND.",
                                    LDOMAIN(recvaddr.c_str()), STRERR);
                        qLogWarnfmt(
                                "RequestProcessor[%s]: this normally indicates filesystem content and in-memory index incoherency.",
                                LDOMAIN(recvaddr.c_str()));
                        qLogWarnfmt("RequestProcessor[%s]: you should recheck the whole process carefully!!",
                                    LDOMAIN(recvaddr.c_str()));
                        rr->type = RequestType::TYPE_EEXIST;
                        StartTimer(&t);
                        int sv = reqmb.sendOne(reinterpret_cast<char *>(rr), sizeof(RequestResponse), &cliun);
                        tp->uds_wr += GetTimeElapsed(&t);
                        if (sv == -1) {
                            qLogFailfmt("ReqeustProcessor[%s]: Send Response fail: %s", LDOMAIN(recvaddr.c_str()),
                                        STRERR);
                            abort();
                        }
                    } else {
                        // read things off it
                        StartTimer(&t);
                        ssize_t rdv = read(valuesfd, rr->value, VAL_SIZE);
                        tp->read_disk += GetTimeElapsed(&t);
                        if (rdv != VAL_SIZE) {
                            qLogWarnfmt(
                                    "RequestProcessor[%s]: read failed or incomplete: %s(%ld), treated as NOT FOUND.",
                                    LDOMAIN(recvaddr.c_str()), STRERR, rdv);
                            qLogWarnfmt(
                                    "RequestProcessor[%s]: this normally indicates filesystem content and in-memory index incoherency.",
                                    LDOMAIN(recvaddr.c_str()));
                            qLogWarnfmt("RequestProcessor[%s]: you should recheck the whole process carefully!!",
                                        LDOMAIN(recvaddr.c_str()));
                            rr->type = RequestType::TYPE_EEXIST;
                            StartTimer(&t);
                            int sv = reqmb.sendOne(reinterpret_cast<char *>(rr), sizeof(RequestResponse), &cliun);
                            tp->uds_wr += GetTimeElapsed(&t);
                            if (sv == -1) {
                                qLogFailfmt("ReqeustProcessor[%s]: Send Response fail: %s", LDOMAIN(recvaddr.c_str()),
                                            STRERR);
                                abort();
                            }
                        } else {
                            // read OK.
                            // release the spyce!
                            qLogDebugfmt("RequestProcessor[%s]: Value found on DISK", LDOMAIN(recvaddr.c_str()));
                            qLogDebugfmt("RequestProcessor[%s]: Value read off disk: %s", LDOMAIN(recvaddr.c_str()), KVArrayDump(rr->value, 2).c_str());
                            rr->type = RequestType::TYPE_OK;
                            StartTimer(&t);
                            int sv = reqmb.sendOne(reinterpret_cast<char *>(rr), sizeof(RequestResponse), &cliun);
                            tp->uds_wr += GetTimeElapsed(&t);
                            if (sv == -1) {
                                qLogFailfmt("ReqeustProcessor[%s]: Send Response fail: %s", LDOMAIN(recvaddr.c_str()),
                                            STRERR);
                                abort();
                            }
                        }
                    }
                }
                qLogDebugfmt("RequestProcessor[%s]: Processing Complete.", LDOMAIN(recvaddr.c_str()));
            } else {
                qLogDebugfmt("ReqeustProcessor[%s]: WR !", LDOMAIN(recvaddr.c_str()));
                qLogDebugfmt("RequestProcessor[%s]: K %hu => V %hu", LDOMAIN(recvaddr.c_str()),
                        *reinterpret_cast<uint16_t*>(rr->key), *reinterpret_cast<uint16_t*>(rr->value));
                // get New Index
                uint64_t file_offset = polar_race::NextIndex.fetch_add(VAL_SIZE);
                // put into GlobIdx
                StartTimer(&t);
                GlobalIndexStore->put(*reinterpret_cast<uint64_t *>(rr->key), file_offset);
                tp->index_put += GetTimeElapsed(&t);
                qLogDebugfmt("RequestProcessor[%s]: WR file_offset %lu !", LDOMAIN(recvaddr.c_str()), file_offset);
                while (*LARRAY_ACCESS(CommitCompletionQueue, file_offset / VAL_SIZE, COMMIT_QUEUE_LENGTH));
                // flush into CommitQueue
                memcpy(LARRAY_ACCESS(CommitQueue, file_offset, COMMIT_QUEUE_LENGTH * VAL_SIZE),
                       rr->value, VAL_SIZE);
                // set CanCommit
                *LARRAY_ACCESS(CommitCompletionQueue, file_offset / VAL_SIZE, COMMIT_QUEUE_LENGTH) = true;
                qLogDebugfmt("RequestProcessor[%s]: CommitCompletionQueue state SET (now %d).",
                             LDOMAIN(recvaddr.c_str()),
                             *LARRAY_ACCESS(CommitCompletionQueue, file_offset / VAL_SIZE, COMMIT_QUEUE_LENGTH));
                // flush OK.
                // wait it gets flush'd
                StartTimer(&t);
                while (*LARRAY_ACCESS(CommitCompletionQueue, file_offset / VAL_SIZE, COMMIT_QUEUE_LENGTH));
                tp->spin_commit += GetTimeElapsed(&t);
                // generate return information.
                qLogDebugfmt("RequestProcessor[%s]: Write transcation committed.", LDOMAIN(recvaddr.c_str()));
                rr->type = RequestType::TYPE_OK;
                StartTimer(&t);
                int sv = reqmb.sendOne(reinterpret_cast<char *>(rr), sizeof(RequestResponse), &cliun);
                tp->uds_wr += GetTimeElapsed(&t);
                if (sv == -1) {
                    qLogFailfmt("ReqeustProcessor[%s]: Send Response fail: %s", LDOMAIN(recvaddr.c_str()), STRERR);
                    abort();
                }
                qLogDebugfmt("RequestProcessor[%s]: Processing Complete.", LDOMAIN(recvaddr.c_str()));
            }
        }
    }

}

