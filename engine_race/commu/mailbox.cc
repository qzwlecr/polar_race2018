#include "commu.h"
#include "../consts/consts.h"
#include "../format/log.h"
#include <cerrno>
#include <unistd.h>

#define LDOMAIN(x) ((x) + 1)
using namespace std;

namespace polar_race {
    MailBox::MailBox() {
        desc = -1;
    }

    struct sockaddr_un mksockaddr_un(const std::string &addr) {
        struct sockaddr_un tmpaddr = {0};
        tmpaddr.sun_family = AF_UNIX;
        memcpy(tmpaddr.sun_path, addr.c_str(),
               min(addr.length(), sizeof(tmpaddr.sun_path) - 1));
        return tmpaddr;
    }

    struct sockaddr_un mksockaddr_un(const char *addr, size_t addrlen) {
        struct sockaddr_un tmpaddr = {0};
        tmpaddr.sun_family = AF_UNIX;
        memcpy(tmpaddr.sun_path, addr,
               min(addrlen, sizeof(tmpaddr.sun_path) - 1));
        return tmpaddr;
    }

    MailBox::MailBox(string &addr) {
        struct sockaddr_un tmpaddr = mksockaddr_un(addr);
        // creating socket
        int sock = socket(AF_UNIX, SOCK_DGRAM, 0);
        if (sock == -1) {
            // socket creation failed
            qLogFailfmt("UDS Creation failed: %s", strerror(errno));
            abort();
        }
        while(true){
            int rv = bind(sock,
                          reinterpret_cast<struct sockaddr *>(&tmpaddr),
                          sizeof(struct sockaddr_un));
            if (rv == -1) {
                if(UDS_BIND_FAIL_SUPRESS){
                    qLogWarnfmt("UDS Bind (%d)[%s] failed: %s", sock, LDOMAIN(addr.c_str()), strerror(errno)); 
                    qLogWarn("Due to configuration, this ERROR is suppressed to Warning, and will retry in 1s.");
                    sleep(1);
                    continue;
                } else {
                    qLogFailfmt("UDS Bind [%s] failed: %s", LDOMAIN(addr.c_str()), strerror(errno)); 
                    abort();
                }
            }
        }
        desc = sock;
        // now this socket is suitable for calling RECVFROM, we stop here.
    }

    MailBox::MailBox(int fd) {
        desc = fd;
    }

    MailBox &MailBox::operator=(int fd) {
        desc = fd;
        return *this;
    }

    int MailBox::open() {
        if (desc >= 0) {
            qLogWarn("Calling MailBox::open on already opened mailbox.");
            return -1;
        }
        int sock = socket(AF_UNIX, SOCK_DGRAM, 0);
        if (sock == -1) {
            // socket creation failed
            qLogWarnfmt("UDS Creation failed: %s", strerror(errno));
            return -1;
        }
        desc = sock;
        return 0;
    }

    ssize_t MailBox::getOne(char *buf, size_t len, struct sockaddr_un *pairAddress) {
        if (desc < 0) {
            qLogWarn("Calling MailBox::getOne on invalid mailbox.");
            errno = EINVAL;
            return -1;
        }
        socklen_t addrlen = sizeof(struct sockaddr_un);
        ssize_t rv = recvfrom(desc,
                              buf, len, 0,
                              reinterpret_cast<struct sockaddr *>(pairAddress), &addrlen);
        if (rv == -1) {
            qLogWarnfmt("MailBox[%d] recvfrom failed: %s", desc, strerror(errno));
            return -1;
        }
        return rv;
    }

    ssize_t MailBox::sendOne(const char *buf, size_t len, const struct sockaddr_un *pairAddress) {
        if (desc < 0) {
            qLogWarn("Calling MailBox::getOne on invalid mailbox.");
            errno = EINVAL;
            return -1;
        }
        socklen_t addrlen = sizeof(struct sockaddr_un);
        ssize_t rv = sendto(desc,
                            buf, len, 0,
                            reinterpret_cast<const struct sockaddr *>(pairAddress), addrlen);
        if (rv == -1) {
            qLogWarnfmt("MailBox[%d] sendto failed: %s", desc, strerror(errno));
            return -1;
        }
        return rv;
    }

    int MailBox::close() {
        int rv = ::close(desc);
        if (rv == -1) {
            qLogWarnfmt("MailBox[%d] close failed: %s", desc, strerror(errno));
            return -1;
        }
        desc = -1;
        return 0;
    }

}
