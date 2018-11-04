#include "task.h"
#include <unistd.h>
#include "../format/log.h"
#include "../index/index.h"
#include <fcntl.h>
#include <iostream>

#if defined(__GNUC__)
#define likely(x) (__builtin_expect((x), 1))
#define unlikely(x) (__builtin_expect((x), 0))
#else
#define likely(x) (x)
#define unlikely(x) (x)
#endif

#define LDOMAIN(x) ((x) + 1)


namespace polar_race {

    using namespace std;
    volatile bool ExitSign = false;

    const uint32_t HB_MAGIC = 0x8088;

    Accumulator NextIndex(0);

#define STRERR (strerror(errno))

    void HeartBeater(string sendaddr, bool *running) {
        qLogSuccfmt("HeartBeater: initialize %s", LDOMAIN(sendaddr.c_str()));
        MailBox hbmb;
        if (unlikely(hbmb.open() == -1)) {
            qLogFailfmt("HeartBeat MailBox open failed: %s", strerror(errno));
            abort();
        }
        struct sockaddr_un un_sendaddr = mksockaddr_un(sendaddr);
        while (*running) {
            if (unlikely(hbmb.sendOne(reinterpret_cast<const char *>(&HB_MAGIC),
                                      sizeof(HB_MAGIC), &un_sendaddr) == -1)) {
                qLogWarnfmt("HeartBeat Send GG?? is receiver GG?? %s", strerror(errno));
                /* abort(); */
            }
            qLogDebug("HeartBeater: beat!");
            sleep(1);
        }
        hbmb.close();
    }

    void HeartBeatChecker(string recvaddr) {
        // ENSURE
        ExitSign = false;
        qLogSuccfmt("HeartBeatChecker: initialize %s", LDOMAIN(recvaddr.c_str()));
        MailBox hbcmb(recvaddr);
        if (unlikely(hbcmb.desc == -1)) {
            qLogFailfmt("HeartBeatChecker MailBox open failed: %s", strerror(errno));
            abort();
        }
        struct sockaddr_un hbaddr = {0};
        Multiplexer mp;
        if (unlikely(mp.open() == -1)) {
            qLogFailfmt("HeartBeatChecker Multiplexer open failed: %s", STRERR);
            abort();
        }
        if (unlikely(mp.listen(hbcmb) == -1)) {
            qLogFailfmt("HeartBeatChecker Multiplexer listen HBMailBox failed: %s", STRERR);
            abort();
        }
        MailBox successer;
        uint32_t hbmagic = 0;
        while (true) {
            int rv = mp.wait(&successer, 1, 3000);
            if (unlikely(rv == -1)) {
                qLogFailfmt("HeartBeatChecker Multiplexer Wait Failed: %s", STRERR);
                continue;
            }
            if (unlikely(rv == 0)) {
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
            if (unlikely(rdv == -1)) {
                qLogFailfmt("HeartBeatChecker unexpected MailBox Get Failure: %s", STRERR);
                ExitSign = true;
                mp.close();
                hbcmb.close();
                return;
            }
        }
    }

#define LARRAY_ACCESS(larr, offset, wrap) ((larr) + ((offset) % (wrap)))
    
    void RequestProcessor(string recvaddr) {
        MailBox reqmb(recvaddr);
        if (unlikely(reqmb.desc == -1)) {
            qLogFailfmt("RequestProcessor recv MailBox open failed: %s", STRERR);
            abort();
        }
        struct sockaddr_un cliun = {0};
        RequestResponse rr = {0};
        int valuesfd = ::open(VALUES_PATH.c_str(), O_NOATIME);
        if (valuesfd == -1) {
            qLogFailfmt("Cannot open values file %s, is it created already??", VALUES_PATH.c_str());
            abort();
        }
        while (true) {
            int gv = reqmb.getOne(reinterpret_cast<char *>(&rr),
                                  sizeof(rr), &cliun);
            if (unlikely(gv == -1)) {
                qLogFailfmt("RequestProcessor[%s]: getRequest failed: %s", LDOMAIN(recvaddr.c_str()), STRERR);
                return;
            }
            // simply ok..
            if (rr.type == RequestType::TYPE_RD) {
                uint64_t key = *reinterpret_cast<uint64_t *>(rr.key);
                uint64_t file_offset = 0;
                qLogDebugfmt("RequestProcessor[%s]: RD %s !", LDOMAIN(recvaddr.c_str()), KVArrayDump(rr.key, 8).c_str());
                // look up in global index store
                if (!global_index_store.get(key, file_offset)) {
                    // not found
                    qLogDebugfmt("RequestProcessor[%s]: Key not found !", LDOMAIN(recvaddr.c_str()));
                    rr.type = RequestType::TYPE_EEXIST;
                    int sv = reqmb.sendOne(reinterpret_cast<char *>(&rr), sizeof(RequestResponse), &cliun);
                    if (sv == -1) {
                        qLogFailfmt("ReqeustProcessor[%s]: Send Response fail: %s", LDOMAIN(recvaddr.c_str()), STRERR);
                        abort();
                    }
                } else {
                    qLogDebugfmt("RequestProcessor[%s]: file_offset %lu, WrittenIdx %lu !", LDOMAIN(recvaddr.c_str()), file_offset, WrittenIndex);
                    // check WrittenIndex against expectedIndex
                    if (file_offset >= WrittenIndex) {
                        // read from internal buffer
                        memcpy(rr.value, LARRAY_ACCESS(InternalBuffer, file_offset,INTERNAL_BUFFER_LENGTH), VAL_SIZE);
                        qLogDebugfmt("RequestProcessor[%s]: rr.value %s !", LDOMAIN(recvaddr.c_str()), KVArrayDump(rr.value, 8).c_str());
                        // check WrittenIndex again
                        if (file_offset >= WrittenIndex) {
                            // then we should return it
                            qLogDebugfmt("RequestProcessor[%s]: Value found on InternalBuffer",
                                        LDOMAIN(recvaddr.c_str()));
                            rr.type = RequestType::TYPE_OK;
                            int sv = reqmb.sendOne(reinterpret_cast<char *>(&rr), sizeof(RequestResponse), &cliun);
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
                        rr.type = RequestType::TYPE_EEXIST;
                        int sv = reqmb.sendOne(reinterpret_cast<char *>(&rr), sizeof(RequestResponse), &cliun);
                        if (sv == -1) {
                            qLogFailfmt("ReqeustProcessor[%s]: Send Response fail: %s", LDOMAIN(recvaddr.c_str()),
                                        STRERR);
                            abort();
                        }
                    } else {
                        // read things off it
                        ssize_t rdv = read(valuesfd, rr.value, VAL_SIZE);
                        if (rdv != VAL_SIZE) {
                            qLogWarnfmt(
                                    "RequestProcessor[%s]: read failed or incomplete: %s(%ld), treated as NOT FOUND.",
                                    LDOMAIN(recvaddr.c_str()), STRERR, rdv);
                            qLogWarnfmt(
                                    "RequestProcessor[%s]: this normally indicates filesystem content and in-memory index incoherency.",
                                    LDOMAIN(recvaddr.c_str()));
                            qLogWarnfmt("RequestProcessor[%s]: you should recheck the whole process carefully!!",
                                        LDOMAIN(recvaddr.c_str()));
                            rr.type = RequestType::TYPE_EEXIST;
                            int sv = reqmb.sendOne(reinterpret_cast<char *>(&rr), sizeof(RequestResponse), &cliun);
                            if (sv == -1) {
                                qLogFailfmt("ReqeustProcessor[%s]: Send Response fail: %s", LDOMAIN(recvaddr.c_str()),
                                            STRERR);
                                abort();
                            }
                        } else {
                            // read OK.
                            // release the spyce!
                            qLogDebugfmt("RequestProcessor[%s]: Value found on DISK", LDOMAIN(recvaddr.c_str()));
                            qLogDebugfmt("RequestProcessor[%s]: Value read off disk: %s", LDOMAIN(recvaddr.c_str()), KVArrayDump(rr.value, 8).c_str());
                            rr.type = RequestType::TYPE_OK;
                            int sv = reqmb.sendOne(reinterpret_cast<char *>(&rr), sizeof(RequestResponse), &cliun);
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
                        *reinterpret_cast<uint16_t*>(rr.key), *reinterpret_cast<uint16_t*>(rr.value));
                // get New Index
                uint64_t file_offset = polar_race::NextIndex.fetch_add(VAL_SIZE);
                // put into GlobIdx
                global_index_store.put(*reinterpret_cast<uint64_t *>(rr.key), file_offset);
                qLogDebugfmt("RequestProcessor[%s]: WR file_offset %lu !", LDOMAIN(recvaddr.c_str()), file_offset);
                while (*LARRAY_ACCESS(CommitCompletionQueue, file_offset / VAL_SIZE, COMMIT_QUEUE_LENGTH) == true);
                // flush into CommitQueue
                memcpy(LARRAY_ACCESS(CommitQueue, file_offset, COMMIT_QUEUE_LENGTH * VAL_SIZE),
                       rr.value, VAL_SIZE);
                // set CanCommit
                *LARRAY_ACCESS(CommitCompletionQueue, file_offset / VAL_SIZE, COMMIT_QUEUE_LENGTH) = true;
                qLogDebugfmt("RequestProcessor[%s]: CommitCompletionQueue state SET (now %d).", LDOMAIN(recvaddr.c_str()),
                        *LARRAY_ACCESS(CommitCompletionQueue, file_offset / VAL_SIZE, COMMIT_QUEUE_LENGTH));
                // flush OK.
                // wait it gets flush'd
                while (*LARRAY_ACCESS(CommitCompletionQueue, file_offset / VAL_SIZE, COMMIT_QUEUE_LENGTH) == true);
                // generate return information.
                qLogDebugfmt("RequestProcessor[%s]: Write transcation committed.", LDOMAIN(recvaddr.c_str()));
                rr.type = RequestType::TYPE_OK;
                int sv = reqmb.sendOne(reinterpret_cast<char *>(&rr), sizeof(RequestResponse), &cliun);
                if (sv == -1) {
                    qLogFailfmt("ReqeustProcessor[%s]: Send Response fail: %s", LDOMAIN(recvaddr.c_str()), STRERR);
                    abort();
                }
                qLogDebugfmt("RequestProcessor[%s]: Processing Complete.", LDOMAIN(recvaddr.c_str()));
            }
        }
    }

}

