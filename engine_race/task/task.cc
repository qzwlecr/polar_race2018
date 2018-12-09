#include "task.h"
#include "../format/log.h"
#include "../index/index.h"
#include "bucket/bucket_link_list.h"
#include "bucket/bucket.h"
#include <algorithm>

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
                for (int i = 0; i < BUCKET_NUMBER; i++) {
                    delete Buckets[i];
                }
                for (int i = 0; i < BUCKET_BACKUP_NUMBER; i++) {
                    free(BackupBuffer[i]);
                }
                // delete buffers

                int index_fd = open(INDECIES_PATH.c_str(), O_CREAT | O_TRUNC | O_RDWR | O_APPEND, 0666);
                int offset_fd = open(OFFSET_TABLE_PATH.c_str(), O_CREAT | O_TRUNC | O_RDWR | O_APPEND, 0666);
                int key_fd = open(KEY_TABLE_PATH.c_str(), O_CREAT | O_TRUNC | O_RDWR | O_APPEND, 0666);
                int meta_fd = open(META_PATH.c_str(), O_CREAT | O_TRUNC | O_RDWR | O_APPEND, 0666);

                GlobalIndexStore->persist(index_fd);
                for (int i = 0; i < HANDLER_THREADS; i++) {
                    std::cout << "--------------------ThreadId = " << i << "----------------" << std::endl;
                    PrintTiming(handtps[i]);
                    std::cout << "--------------------ThreadId = " << i << "----------------" << std::endl;
                }

                qLogDebugfmt("HeartBeatChecker: slot start = %lx, length = %lu, slot end = %lx",
                             (uint64_t)&(GlobalIndexStore->hashmap.slots_),
                             GlobalIndexStore->hashmap.numSlots_,
                             (unsigned long) (&(GlobalIndexStore->hashmap.slots_) +
                                              GlobalIndexStore->hashmap.numSlots_ *
                                              sizeof(AtomicUnorderedInsertMap::Slot))
                );

                std::sort(GlobalIndexStore->hashmap.slots_ + 1,
                          GlobalIndexStore->hashmap.slots_ + GlobalIndexStore->hashmap.numSlots_ + 1);

                qLogDebug("HeartBeatChecker: Sort Done.");

                uint32_t *sorted_offset = (uint32_t *) malloc(HASH_MAP_SIZE * sizeof(uint32_t));
                uint64_t index = 0;
                for (auto iter = GlobalIndexStore->hashmap.cbegin(); iter != GlobalIndexStore->hashmap.cend(); iter++) {
                    sorted_offset[index++] = (*iter).second.data;
                }
                write(offset_fd, sorted_offset, index * sizeof(uint32_t));
                free(sorted_offset);

                uint64_t *sorted_key = (uint64_t *) malloc(HASH_MAP_SIZE * sizeof(uint64_t));
                index = 0;
                for (auto iter = GlobalIndexStore->hashmap.cbegin(); iter != GlobalIndexStore->hashmap.cend(); iter++) {
                    sorted_key[index++] = (*iter).first;
                }
                write(key_fd, sorted_key, index * sizeof(uint64_t));
                free(sorted_key);

                uint64_t file_size = NextIndex.load();
                write(meta_fd, &file_size, sizeof(uint64_t));
                write(meta_fd, &index, sizeof(uint64_t));
                BucketLinkList::persist(meta_fd);

                close(index_fd);
                close(key_fd);
                close(offset_fd);
                close(meta_fd);

                flock(lockfd, LOCK_UN);
                qLogSucc("Flusher unlocked filelock");
                mp.close();
                hbcmb.close();
                exit(0);
            }
            // not very ok exactly..
            ssize_t rdv = hbcmb.getOne(reinterpret_cast<char *>(&hbmagic), sizeof(HB_MAGIC), &hbaddr);
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
//        if (posix_fadvise64(valuesfd, 0, 0, POSIX_FADV_RANDOM) == -1) {
//            qLogFailfmt("RequestProcessor: Cannot advise file usage pattern: %s", strerror(errno));
//            abort();
//        }
        struct timespec t = {0};
        while (true) {
//            if (UNLIKELY(!attached)) {
//                cpu_set_t set;
//                CPU_ZERO(&set);
//                CPU_SET(own_id * 2, &set);
//                //dirty hard code
//                if (sched_setaffinity(0, sizeof(set), &set) == -1) {
//                    qLogFailfmt("RequestProcessor sched set affinity failed: %s", STRERR);
//                    abort();
//                }
//                attached = true;
//            }
            StartTimer(&t);
            ssize_t gv = reqmb.getOne(reinterpret_cast<char *>(rr), sizeof(RequestResponse), &cliun);
            tp->uds_rd += GetTimeElapsed(&t);
            if (UNLIKELY(gv != sizeof(RequestResponse))) {
                qLogFailfmt("RequestProcessor[%s]: getRequest failed or incomplete: %s(%ld)", LDOMAIN(recvaddr.c_str()),
                            STRERR, gv);
                continue;
            }
            qLogDebugfmt("ReqeustProcessor[%s]: WR !", LDOMAIN(recvaddr.c_str()));
            qLogDebugfmt("RequestProcessor[%s]: K %s => V %s", LDOMAIN(recvaddr.c_str()),
                         KVArrayDump(rr->key, 8).c_str(), KVArrayDump(rr->value, 8).c_str());
            uint64_t file_offset;
            // put into GlobIdx
            StartTimer(&t);
            Buckets[(*reinterpret_cast<uint64_t *>(rr->key)) / (UINT64_MAX / BUCKET_NUMBER)]->put(file_offset,
                                                                                                  rr->value);
            qLogDebugfmt("RequestProcessor[%s]: Bucket OK, selected is %lu", LDOMAIN(recvaddr.c_str()),
            (*reinterpret_cast<uint64_t *>(rr->key)) / (UINT64_MAX / BUCKET_NUMBER));
            tp->spin_commit += GetTimeElapsed(&t);
            StartTimer(&t);
            GlobalIndexStore->put(*reinterpret_cast<uint64_t *>(rr->key), file_offset);
            tp->index_put += GetTimeElapsed(&t);
            qLogDebugfmt("RequestProcessor[%s]: WR file_offset %lu !", LDOMAIN(recvaddr.c_str()), file_offset);
            // generate return information.
            qLogDebugfmt("RequestProcessor[%s]: Write transcation committed.", LDOMAIN(recvaddr.c_str()));
            rr->type = RequestType::TYPE_OK;
            StartTimer(&t);
            ssize_t sv = reqmb.sendOne(reinterpret_cast<char *>(rr), sizeof(RequestResponse), &cliun);
            tp->uds_wr += GetTimeElapsed(&t);
            if (sv == -1) {
                qLogFailfmt("ReqeustProcessor[%s]: Send Response fail: %s", LDOMAIN(recvaddr.c_str()), STRERR);
                abort();
            }
            qLogDebugfmt("RequestProcessor[%s]: Processing Complete.", LDOMAIN(recvaddr.c_str()));
        }
    }

}

