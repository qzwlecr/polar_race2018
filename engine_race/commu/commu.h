#ifndef ENGINE_RACE_COMMU_COMMU_H
#define ENGINE_RACE_COMMU_COMMU_H

#include <cstdint>
#include <string>
#include <sys/un.h>
#include <sys/socket.h>
#include <cctype>
#include <atomic>

namespace polar_race {
    using std::size_t;
    struct sockaddr_un mksockaddr_un(const std::string& addr);
    struct sockaddr_un mksockaddr_un(const char* addr, size_t addrlen);
    using Accumulator=std::atomic_uint64_t;
    class MailBox {
        public:
            MailBox();
            MailBox(std::string& address); // only for recving socket
            MailBox(int fdesc);
            MailBox& operator=(int fd);
            int open(); // only for sending socket
            ssize_t getOne(char* buf, size_t len, struct sockaddr_un* pairAddress);
            ssize_t sendOne(const char* buf, size_t len, const struct sockaddr_un* pairAddress);
            int close();
            int desc;
    };
    class Multiplexer {
        public:
            Multiplexer();
            int open();
            int listen(const MailBox& mailbox); // listen for receiving
            int unlisten(const MailBox& mailbox); // unlisten
            int wait(MailBox* succeed, int maxevs, int timeout); // unit ms
            int close();
            int desc;
    };
};


#endif
