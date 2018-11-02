#include "task.h"
#include "../commu/commu.h"
#include "unistd.h"
#include "../format/log.h"
#include "../index/index.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#if defined(__GNUC__)
#define likely(x) (__builtin_expect((x), 1))
#define unlikely(x) (__builtin_expect((x), 0))
#else
#define likely(x) (x)
#define unlikely(x) (x)
#endif


using namespace std;
using namespace polar_race;

bool polar_race::ExitSign = false;

const uint32_t HB_MAGIC = 0x8088;

#define STRERR (strerror(errno))

void HeartBeater(string sendaddr){
    MailBox hbmb;
    if(unlikely(hbmb.open() == -1)){
        qLogFailfmt("HeartBeat MailBox open failed: %s", strerror(errno));
        abort();
    }
    struct sockaddr_un un_sendaddr = mksockaddr_un(sendaddr);
    while(true){
        if(unlikely(hbmb.sendOne(reinterpret_cast<const char*>(&HB_MAGIC),
                    sizeof(HB_MAGIC), &un_sendaddr) == -1)){
            qLogFailfmt("HeartBeat Send GG?? is receiver GG?? %s", strerror(errno));
            abort();
        }
        sleep(1);
    }
}

void HeartBeatChecker(string recvaddr){
    // ENSURE
    ExitSign = false;
    MailBox hbcmb(recvaddr);
    if(unlikely(hbcmb.desc == -1)){
        qLogFailfmt("HeartBeatChecker MailBox open failed: %s", strerror(errno));
        abort();
    }
    struct sockaddr_un hbaddr = {0};
    Multiplexer mp;
    if(unlikely(mp.open() == -1)){
        qLogFailfmt("HeartBeatChecker Multiplexer open failed: %s", STRERR);
        abort();
    }
    if(unlikely(mp.listen(hbcmb) == -1)){
        qLogFailfmt("HeartBeatChecker Multiplexer listen HBMailBox failed: %s", STRERR);
        abort();
    }
    MailBox successer;
    uint32_t hbmagic = 0;
    while(true){
        int rv = mp.wait(&successer, 1, 2000);
        if(unlikely(rv == -1)){
            qLogFailfmt("HeartBeatChecker Multiplexer Wait Failed: %s", STRERR);
            if(errno != EINTR){
                abort();
            }
            continue;
        }
        if(unlikely(rv == 0)){
            // timed out!
            ExitSign = true;
            // do clean work
            mp.close();
            hbcmb.close();
            return;
        }
        // not very ok exactly..
        int rdv = hbcmb.getOne(reinterpret_cast<char*>(&hbmagic), sizeof(HB_MAGIC), &hbaddr);
        if(unlikely(rdv == -1)){
            qLogFailfmt("HeartBeatChecker unexpected MailBox Get Failure: %s", STRERR);
            ExitSign = true;
            mp.close();
            hbcmb.close();
            return;
        }
    }
}

#define LDOMAIN(x) ((x) + 1)
#define LARRAY_ACCESS(larr, offset, wrap) ((larr) + ((offset) % (wrap)))

void RequestProcessor(string recvaddr){
    MailBox reqmb(recvaddr);
    if(unlikely(reqmb.desc == -1)){
        qLogFailfmt("RequestProcessor recv MailBox open failed: %s", STRERR);
        abort();
    }
    struct sockaddr_un cliun = {0};
    RequestResponse rr = {0};
    int valuesfd = ::open(VALUES_PATH.c_str(), O_NOATIME);    
    if(valuesfd == -1){
        qLogFailfmt("Cannot open values file %s, is it created already??", VALUES_PATH.c_str());
        abort();
    }
    while(true){
        int gv = reqmb.getOne(reinterpret_cast<char*>(&rr),
                sizeof(rr), &cliun);
        if(unlikely(gv == -1)){
            qLogFailfmt("RequestProcessor[%s]: getRequest failed: %s", LDOMAIN(recvaddr.c_str()), STRERR);
            return;
        }
        // simply ok..
        if(rr.type == RequestType::TYPE_RD){
            uint64_t key = *reinterpret_cast<uint64_t*>(rr.key);
            uint64_t file_offset = 0;
            qLogInfofmt("RequestProcessor[%s]: RD %lx !", LDOMAIN(recvaddr.c_str()), key);
            // look up in global index store
            if(!global_index_store.get(key, file_offset)){
                // not found
                qLogInfofmt("RequestProcessor[%s]: Key not found !", LDOMAIN(recvaddr.c_str()));
                rr.type = RequestType::TYPE_EEXIST;
                int sv = reqmb.sendOne(reinterpret_cast<char*>(&rr), sizeof(RequestResponse), &cliun);
                if(sv == -1){
                    qLogFailfmt("ReqeustProcessor[%s]: Send Response fail: %s", LDOMAIN(recvaddr.c_str()), STRERR);
                    abort();
                }
            } else {
                // check WrittenIndex against expectedIndex
                if(file_offset > WrittenIndex){
                    // read from internal buffer
                    memcpy(rr.value, LARRAY_ACCESS(InternalBuffer, file_offset, INTERNAL_BUFFER_LENGTH), VAL_SIZE);
                }
            }
            qLogInfofmt("RequestProcessor[%s]: Processing Complete.", LDOMAIN(recvaddr.c_str()));
        } else {
            qLogInfofmt("ReqeustProcessor[%s]: WR !", LDOMAIN(recvaddr.c_str()));
            
        }
    }
}



