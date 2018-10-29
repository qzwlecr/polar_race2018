#include "commu.h"
#include "../format/log.h"
#include <sys/epoll.h>
#include <cerrno>
#include <cstring>
#include <unistd.h>

using namespace ZHWKLib;
using namespace std;

Multiplexer::Multiplexer(){
    desc = -1;
}

int Multiplexer::open(){
    if(desc != -1){
        qLogWarn("Reopening opened Multiplexer");
        return -1;
    }
    desc = epoll_create(256);
    if(desc == -1){
        qLogWarnfmt("Multiplexer opening failed: %s", strerror(errno));
        return -1;
    }
    return 0;
}

int Multiplexer::listen(const MailBox& mailbox){
    if(desc < 0){
        qLogWarn("Operating on an invalid Multiplexer");
        errno = EINVAL;
        return -1;
    }
    struct epoll_event ev = {
        .events = EPOLLIN | EPOLLRDHUP,
        .data.fd = mailbox.desc
    };
    int rv = epoll_ctl(desc, EPOLL_CTL_ADD, mailbox.desc, &ev);
    if(rv == -1){
        qLogWarnfmt("Multiplexer[%d] listen %d failed: %s", desc, mailbox.desc, strerror(errno));
        return -1;
    }
    return 0;
}

int Multiplexer::unlisten(const MailBox& mailbox){
    if(desc < 0){
        qLogWarn("Operating on an invalid Multiplexer");
        errno = EINVAL;
        return -1;
    }
    int rv = epoll_ctl(desc, EPOLL_CTL_DEL, mailbox.desc, NULL);
    if(rv == -1){
        qLogWarnfmt("Multiplexer[%d] unlisten %d failed: %s", desc, mailbox.desc, strerror(errno));
        return -1;
    }
    return 0;
}

int Multiplexer::wait(MailBox* succArray, int maxevs, int timeout){
    if(desc < 0){
        qLogWarn("Operating on an invalid Multiplexer");
        errno = EINVAL;
        return -1;
    }
    struct epoll_event evs[maxevs];
    memset(evs, 0, maxevs * sizeof(struct epoll_event));
    int rv = epoll_wait(desc, evs, maxevs, timeout);
    if(rv == -1){
        qLogWarnfmt("Multiplexer[%d] wait failed: %s", desc, strerror(errno));
        return -1;
    }
    // fill the return array
    for(int i = 0; i < rv; i++){
        succArray[i] = evs[i].data.fd;
    }
    return rv;
}

int Multiplexer::close(){
    if(desc < 0){
        qLogWarn("Operating on an invalid Multiplexer");
        errno = EINVAL;
        return -1;
    }
    int rv = ::close(desc);
    if(rv == -1){
        qLogWarnfmt("Multiplexer[%d] close failed: %s", desc, strerror(errno));
        return -1;
    }
    return rv;
}

