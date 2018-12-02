#include "task.h"
#include "../format/log.h"
#include "../index/index.h"
#include "bucket/bucket_link_list.h"
#include "bucket/bucket.h"

extern "C" {
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
#define LARRAY_ACCESS(larr, offset, wrap) ((larr) + ((offset) % (wrap)))

    volatile bool ExitSign = false;

    const uint32_t HB_MAGIC = 0x8088;

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
                ExitSign = true;
                // do clean work
                delete BucketThreadPool;
                uint64_t file_size = NextIndex.load();
                int meta_fd = open(META_PATH.c_str(), O_CREAT | O_TRUNC | O_RDWR | O_APPEND, 0666);
                write(meta_fd, &file_size, sizeof(uint64_t));
                BucketLinkList::persist(meta_fd);
                int index_fd = open(INDECIES_PATH.c_str(), O_CREAT | O_TRUNC | O_RDWR | O_APPEND, 0666);
                GlobalIndexStore->persist(index_fd);
                for (int i = 0; i < HANDLER_THREADS; i++) {
                    std::cout << "--------------------ThreadId = " << i << "----------------" << std::endl;
                    PrintTiming(handtps[i]);
                    std::cout << "--------------------ThreadId = " << i << "----------------" << std::endl;
                }
                close(index_fd);
                close(meta_fd);
                flock(lockfd, LOCK_UN);
                qLogSucc("Flusher unlocked filelock");
                BucketLinkList::persist(MetaFd);
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

    void RequestProcessor(std::string recvaddr, TimingProfile *tp, uint8_t own_id) {
        bool attached = false;
        MailBox reqmb(recvaddr);
        if (UNLIKELY(reqmb.desc == -1)) {
            qLogFailfmt("RequestProcessor recv MailBox open failed: %s", STRERR);
            abort();
        }
        struct sockaddr_un cliun = {0};
        RequestResponse *rr = reinterpret_cast<RequestResponse *>(memalign(4096, sizeof(RequestResponse)));
        int valuesfd = ::open(VALUES_PATH.c_str(), O_NOATIME);
        if (valuesfd == -1) {
            qLogFailfmt("Cannot open values file %s, is it created already??", VALUES_PATH.c_str());
            abort();
        }
//        if (posix_fadvise64(valuesfd, 0, 0, POSIX_FADV_RANDOM) == -1) {
//            qLogFailfmt("RequestProcessor: Cannot advise file usage pattern: %s", strerror(errno));
//            abort();
//        }
        struct timespec t = {0};
        while (true) {
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
                    // not found
                    qLogInfofmt("RequestProcessor[%s]: Key not found !", LDOMAIN(recvaddr.c_str()));
                    rr->type = RequestType::TYPE_EEXIST;
                    StartTimer(&t);
                    int sv = reqmb.sendOne(reinterpret_cast<char *>(rr), sizeof(RequestResponse), &cliun);
                    tp->uds_wr += GetTimeElapsed(&t);
                    if (UNLIKELY(sv == -1)) {
                        qLogFailfmt("ReqeustProcessor[%s]: Send Response fail: %s", LDOMAIN(recvaddr.c_str()), STRERR);
                        abort();
                    }
                } else {
                    tp->index_get += GetTimeElapsed(&t);
                    // that means we should read it from file
                    int seekv = lseek(valuesfd, file_offset, SEEK_SET);
                    if (UNLIKELY(seekv == -1)) {
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
                        if (UNLIKELY(sv == -1)) {
                            qLogFailfmt("ReqeustProcessor[%s]: Send Response fail: %s", LDOMAIN(recvaddr.c_str()),
                                        STRERR);
                            abort();
                        }
                    } else {
                        // read things off it
                        StartTimer(&t);
                        ssize_t rdv = read(valuesfd, rr->value, VAL_SIZE);
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
                            StartTimer(&t);
                            int sv = reqmb.sendOne(reinterpret_cast<char *>(rr), sizeof(RequestResponse), &cliun);
                            tp->uds_wr += GetTimeElapsed(&t);
                            if (UNLIKELY(sv == -1)) {
                                qLogFailfmt("ReqeustProcessor[%s]: Send Response fail: %s", LDOMAIN(recvaddr.c_str()),
                                            STRERR);
                                abort();
                            }
                        } else {
                            // read OK.
                            // release the spyce!
                            qLogDebugfmt("RequestProcessor[%s]: Value found on DISK", LDOMAIN(recvaddr.c_str()));
                            qLogDebugfmt("RequestProcessor[%s]: Value read off disk: %s", LDOMAIN(recvaddr.c_str()),
                                         KVArrayDump(rr->value, 2).c_str());
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
                if (UNLIKELY(!attached)) {
                    cpu_set_t set;
                    CPU_ZERO(&set);
                    CPU_SET(own_id * 2, &set);
                    //dirty hard code
                    if (sched_setaffinity(0, sizeof(set), &set) == -1) {
                        qLogFailfmt("RequestProcessor sched set affinity failed: %s", STRERR);
                        abort();
                    }
                    attached = true;
                }
                qLogDebugfmt("ReqeustProcessor[%s]: WR !", LDOMAIN(recvaddr.c_str()));
                qLogDebugfmt("RequestProcessor[%s]: K %hu => V %hu", LDOMAIN(recvaddr.c_str()),
                             *reinterpret_cast<uint16_t *>(rr->key), *reinterpret_cast<uint16_t *>(rr->value));
                uint64_t file_offset;
                // put into GlobIdx
                StartTimer(&t);
                Buckets[*reinterpret_cast<uint64_t *>(rr->key)]->put(file_offset, rr->value);
                tp->spin_commit += GetTimeElapsed(&t);
                StartTimer(&t);
                GlobalIndexStore->put(*reinterpret_cast<uint64_t *>(rr->key), file_offset);
                tp->index_put += GetTimeElapsed(&t);
                qLogDebugfmt("RequestProcessor[%s]: WR file_offset %lu !", LDOMAIN(recvaddr.c_str()), file_offset);
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

