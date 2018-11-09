#include "task.h"
#include "../format/log.h"
#include "../index/index.h"
#include "../flusher/flusher.h"
#include "../consts/consts.h"
#include "../commu/commu.h"

extern "C"{
#include <fcntl.h>
#include <unistd.h>
#include <malloc.h>
}

namespace polar_race {

#define STRERR (strerror(errno))
#define LDOMAIN(x) ((x) + 1)

    volatile bool PreExitSign = false;
    volatile bool ExitSign = false;

    const uint32_t HB_MAGIC = 0x8088;

    Accumulator NextIndex(0);
    Accumulator TermCounter(0);

    void SelfCloser(int timeout, bool *running) {
        int timed = 0;
        while (*running) {
            sleep(1);
            timed += 1;
            if (timed >= timeout) {
                qLogFail("SelfCloser: Sanity execution time exceeded.");
                qLogFail("SelfCloser: Forcibly termination..");
                qLogFailfmt("SelfCloser: Current requestIndex is %lu", (uint64_t)requestId);
                qLogFailfmt("SelfCloser: Current completedRd is %lu", (uint64_t)completeRd);
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

#define LARRAY_ACCESS(larr, offset, wrap) ((larr) + ((offset) % (wrap)))

    void RequestProcessor(std::string recvaddr, TimingProfile *tp) {
        MailBox reqmb(recvaddr);
        if (UNLIKELY(reqmb.desc == -1)) {
            qLogFailfmt("RequestProcessor recv MailBox open failed: %s", STRERR);
            abort();
        }
        struct sockaddr_un cliun = {0};
        RequestResponse* rraw = reinterpret_cast<RequestResponse*>(memalign(4096, sizeof(RequestResponse)));
        ReadResponse rresp = {0};
        ReadResponseFull rrespf = {0};
        WriteResponse wresp = {0};
        int valuesfd = ::open(VALUES_PATH.c_str(), O_NOATIME);
        if (valuesfd == -1) {
            qLogFailfmt("Cannot open values file %s, is it created already??", VALUES_PATH.c_str());
            abort();
        }
        struct timespec t = {0};
        Multiplexer mp;
        if (UNLIKELY(mp.open() == -1)) {
            qLogFailfmt("HeartBeatChecker Multiplexer open failed: %s", STRERR);
            abort();
        }
        if (UNLIKELY(mp.listen(reqmb) == -1)) {
            qLogFailfmt("HeartBeatChecker Multiplexer listen HBMailBox failed: %s", STRERR);
            abort();
        }
        MailBox successer;
        while (true) {
            int rv = mp.wait(&successer, 1, 1000);
            if (UNLIKELY(rv == -1)) {
                qLogFailfmt("RequestProcessor[%s]: Multiplexer Wait Failed: %s",
                            LDOMAIN(recvaddr.c_str()), STRERR);
                continue;
            }
            if (UNLIKELY(rv == 0)) {
                if (PreExitSign == true) {
                    qLogSuccfmt("RequestProcessor[%s]: Exiting..", LDOMAIN(recvaddr.c_str()));
                    TermCounter.fetch_add(1);
                    if (TermCounter == HANDLER_THREADS) {
                        qLogSuccfmt("RequestProcessor[%s]: Exiting Gracefully", LDOMAIN(recvaddr.c_str()));
                        ExitSign = true;
                    }
                    return;
                }
                qLogDebugfmt("RequestProcessor[%s]: Timed out, but continue..", LDOMAIN(recvaddr.c_str()));
                continue;
            }
            StartTimer(&t);
            ssize_t gv = reqmb.getOne(reinterpret_cast<char *>(rraw),
                                  sizeof(RequestResponse), &cliun);
            tp->uds_rd += GetTimeElapsed(&t);
            // we are expect to get one of ReadRequest or WriteRequest
            if (UNLIKELY(gv != sizeof(ReadRequest) && gv != sizeof(WriteRequest))) {
                qLogFailfmt("RequestProcessor[%s]: getRequest failed or incomplete: %s(%ld)", LDOMAIN(recvaddr.c_str()), STRERR, gv);
                continue;
            }
            // simply ok..
            if (gv == sizeof(ReadRequest)) {
                ReadRequest* rr = reinterpret_cast<ReadRequest*>(rraw);
                uint64_t key = *reinterpret_cast<uint64_t *>(rr->key);
                uint64_t file_offset = 0;
                qLogInfofmt("RequestProcessor[%s]: RD %s !", LDOMAIN(recvaddr.c_str()),
                            KVArrayDump(rr->key, 2).c_str());
                // look up in global index store
                StartTimer(&t);
                if (!GlobalIndexStore->get(key, file_offset)) {
                    tp->index_get += GetTimeElapsed(&t);
                    // not found
                    qLogInfofmt("RequestProcessor[%s]: Key not found !", LDOMAIN(recvaddr.c_str()));
                    rresp.type = RequestType::TYPE_EEXIST;
                    StartTimer(&t);
                    int sv = reqmb.sendOne(reinterpret_cast<char *>(&rresp), sizeof(ReadResponse), &cliun);
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
                        memcpy(rrespf.value, LARRAY_ACCESS(InternalBuffer, file_offset,INTERNAL_BUFFER_LENGTH), VAL_SIZE);
                        qLogDebugfmt("RequestProcessor[%s]: on InternalBuffer, rr.value %s !", LDOMAIN(recvaddr.c_str()), KVArrayDump(rrespf.value, 2).c_str());
                        // check WrittenIndex again
                        if (file_offset >= WrittenIndex) {
                            // then we should return it
                            qLogDebugfmt("RequestProcessor[%s]: Value found on InternalBuffer",
                                        LDOMAIN(recvaddr.c_str()));
                            rrespf.type = RequestType::TYPE_OK;
                            StartTimer(&t);
                            int sv = reqmb.sendOne(reinterpret_cast<char *>(&rrespf), sizeof(ReadResponseFull), &cliun);
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
                    // we send things directly back...
                    rresp.type = RequestType::TYPE_OK;
                    rresp.foffset = file_offset;
                    qLogDebugfmt("RequestProcessor[%s]: on disk.", LDOMAIN(recvaddr.c_str()));
                    StartTimer(&t);
                    int sv = reqmb.sendOne(reinterpret_cast<char *>(&rresp), sizeof(ReadResponse), &cliun);
                    tp->uds_wr += GetTimeElapsed(&t);
                    if (sv == -1) {
                        qLogFailfmt("ReqeustProcessor[%s]: Send Response fail: %s", LDOMAIN(recvaddr.c_str()),
                                    STRERR);
                        abort();
                    }
                    /* int seekv = lseek(valuesfd, file_offset, SEEK_SET); */
                    /* if (seekv == -1) { */
                    /*     qLogWarnfmt("RequestProcessor[%s]: lseek failed: %s, treated as NOT FOUND.", */
                    /*                 LDOMAIN(recvaddr.c_str()), STRERR); */
                    /*     qLogWarnfmt( */
                    /*             "RequestProcessor[%s]: this normally indicates filesystem content and in-memory index incoherency.", */
                    /*             LDOMAIN(recvaddr.c_str())); */
                    /*     qLogWarnfmt("RequestProcessor[%s]: you should recheck the whole process carefully!!", */
                    /*                 LDOMAIN(recvaddr.c_str())); */
                    /*     rresp.type = RequestType::TYPE_EEXIST; */
                    /*     StartTimer(&t); */
                    /*     int sv = reqmb.sendOne(reinterpret_cast<char *>(&rresp), sizeof(ReadResponse), &cliun); */
                    /*     tp->uds_wr += GetTimeElapsed(&t); */
                    /*     if (sv == -1) { */
                    /*         qLogFailfmt("ReqeustProcessor[%s]: Send Response fail: %s", LDOMAIN(recvaddr.c_str()), */
                    /*                     STRERR); */
                    /*         abort(); */
                    /*     } */
                    /* } else { */
                    /*     // read things off it */
                    /*     StartTimer(&t); */
                    /*     ssize_t rdv = read(valuesfd, rrespf.value, VAL_SIZE); */
                    /*     tp->read_disk += GetTimeElapsed(&t); */
                    /*     if (rdv != VAL_SIZE) { */
                    /*         qLogWarnfmt( */
                    /*                 "RequestProcessor[%s]: read failed or incomplete: %s(%ld), treated as NOT FOUND.", */
                    /*                 LDOMAIN(recvaddr.c_str()), STRERR, rdv); */
                    /*         qLogWarnfmt( */
                    /*                 "RequestProcessor[%s]: this normally indicates filesystem content and in-memory index incoherency.", */
                    /*                 LDOMAIN(recvaddr.c_str())); */
                    /*         qLogWarnfmt("RequestProcessor[%s]: you should recheck the whole process carefully!!", */
                    /*                     LDOMAIN(recvaddr.c_str())); */
                    /*         rresp.type = RequestType::TYPE_EEXIST; */
                    /*         StartTimer(&t); */
                    /*         int sv = reqmb.sendOne(reinterpret_cast<char *>(&rresp), sizeof(ReadResponse), &cliun); */
                    /*         tp->uds_wr += GetTimeElapsed(&t); */
                    /*         if (sv == -1) { */
                    /*             qLogFailfmt("ReqeustProcessor[%s]: Send Response fail: %s", LDOMAIN(recvaddr.c_str()), */
                    /*                         STRERR); */
                    /*             abort(); */
                    /*         } */
                    /*     } else { */
                    /*         // read OK. */
                    /*         // release the spyce! */
                    /*         qLogDebugfmt("RequestProcessor[%s]: Value found on DISK", LDOMAIN(recvaddr.c_str())); */
                    /*         qLogDebugfmt("RequestProcessor[%s]: Value read off disk: %s", LDOMAIN(recvaddr.c_str()), KVArrayDump(rrespf.value, 2).c_str()); */
                    /*         rrespf.type = RequestType::TYPE_OK; */
                    /*         StartTimer(&t); */
                    /*         int sv = reqmb.sendOne(reinterpret_cast<char *>(&rrespf), sizeof(ReadResponseFull), &cliun); */
                    /*         tp->uds_wr += GetTimeElapsed(&t); */
                    /*         if (sv == -1) { */
                    /*             qLogFailfmt("ReqeustProcessor[%s]: Send Response fail: %s", LDOMAIN(recvaddr.c_str()), */
                    /*                         STRERR); */
                    /*             abort(); */
                    /*         } */
                    /*     } */
                    /* } */
                }
                qLogDebugfmt("RequestProcessor[%s]: Processing Complete.", LDOMAIN(recvaddr.c_str()));
            } else {
                WriteRequest* rr = reinterpret_cast<WriteRequest*>(rraw);
                qLogDebugfmt("ReqeustProcessor[%s]: WR !", LDOMAIN(recvaddr.c_str()));
                qLogDebugfmt("RequestProcessor[%s]: K %hu => V %hu", LDOMAIN(recvaddr.c_str()),
                             *reinterpret_cast<uint16_t *>(rr->key), *reinterpret_cast<uint16_t *>(rr->value));
                // get New Index
                uint64_t file_offset = polar_race::NextIndex.fetch_add(VAL_SIZE);
                // put into GlobIdx
                StartTimer(&t);
                GlobalIndexStore->put(*reinterpret_cast<uint64_t *>(rr->key), file_offset);
                tp->index_put += GetTimeElapsed(&t);
                qLogDebugfmt("RequestProcessor[%s]: WR file_offset %lu !", LDOMAIN(recvaddr.c_str()), file_offset);
                uint8_t fake_swap;
                do{
                    fake_swap = COMMIT_COMPLETION_EMPTY;
                } while (LARRAY_ACCESS(CommitCompletionQueue, file_offset / VAL_SIZE, COMMIT_QUEUE_LENGTH)->
                    compare_exchange_weak(fake_swap, COMMIT_COMPLETION_OCCUPIED));
                // flush into CommitQueue
                memcpy(LARRAY_ACCESS(CommitQueue, file_offset, COMMIT_QUEUE_LENGTH * VAL_SIZE),
                       rr->value, VAL_SIZE);
                // set CanCommit
                LARRAY_ACCESS(CommitCompletionQueue, file_offset / VAL_SIZE,
                              COMMIT_QUEUE_LENGTH)->store(COMMIT_COMPLETION_FULL);
                qLogDebugfmt("RequestProcessor[%s]: CommitCompletionQueue state SET (now %d).",
                             LDOMAIN(recvaddr.c_str()),
                             LARRAY_ACCESS(CommitCompletionQueue, file_offset / VAL_SIZE, COMMIT_QUEUE_LENGTH)->load());
                // flush OK.
                // wait it gets flush'd
                /* StartTimer(&t); */
                /* while (*LARRAY_ACCESS(CommitCompletionQueue, file_offset / VAL_SIZE, COMMIT_QUEUE_LENGTH)); */
                /* tp->spin_commit += GetTimeElapsed(&t); */
                // generate return information.
                qLogDebugfmt("RequestProcessor[%s]: Write transaction committed.", LDOMAIN(recvaddr.c_str()));
                wresp.type = RequestType::TYPE_OK;
                StartTimer(&t);
                int sv = reqmb.sendOne(reinterpret_cast<char *>(&wresp), sizeof(WriteResponse), &cliun);
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

