#include "sys.h"

#include <cstdlib>
#include "../format/format.h"
#include "../format/log.h"

#include <cerrno>
#include <cstring>

using namespace std;

namespace polar_race{

extern mutex logMu;

RWMutex::RWMutex(){
    int rv = pthread_rwlock_init(&lockself, 0);
    if(rv != 0){
        qLogFail("Cannot acquire rwlock.");
        abort();
    }
}

RWMutex::RWMutex(const RWMutex& src){
    lockself = src.lockself;
}

RWMutex& RWMutex::operator=(const RWMutex &src){
    lockself = src.lockself;
    return *this;
}

void RWMutex::rdlock(){
    rdlock_retry:
    int rv = pthread_rwlock_rdlock(&lockself);
    if(rv != 0){
        if(rv == EAGAIN) goto rdlock_retry;
        qLogFailfmt("Cannot readlock: %s", strerror(rv));
        abort();
    }
}

void RWMutex::wrlock(){
    wrlock_retry:
    int rv = pthread_rwlock_wrlock(&lockself);
    if(rv != 0){
        if(rv == EAGAIN) goto wrlock_retry;
        qLogFailfmt("Cannot writelock: %s", strerror(rv));
        abort();
    }
}

void RWMutex::unlock(){
    unlock_retry:
    int rv = pthread_rwlock_unlock(&lockself);
    if(rv != 0){
        if(rv == EAGAIN) goto unlock_retry;
        qLogWarnfmt("Cannot unlock: %s", strerror(rv));
    }
}

RWMutex::~RWMutex(){
    pthread_rwlock_destroy(&lockself);
}


};