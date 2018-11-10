#ifndef ENGINE_RACE_COMMU_COMMU_H
#define ENGINE_RACE_COMMU_COMMU_H

#include <cstdint>
#include <string>
#include <sys/un.h>
#include <sys/socket.h>
#include <cctype>
#include <atomic>
#include "../consts/consts.h"

namespace polar_race {
    using std::size_t;

    struct sockaddr_un mksockaddr_un(const std::string &addr);

    struct sockaddr_un mksockaddr_un(const char *addr, size_t addrlen);

    using Accumulator=typename std::atomic<uint64_t>;
    namespace RequestType {
        const uint8_t TYPE_RD = 0;
        const uint8_t TYPE_WR = 1;
        const uint8_t TYPE_OK = 0;
        const uint8_t TYPE_BUSY = 1;
        const uint8_t TYPE_EEXIST = -1;
    };
    struct RequestResponse {
        char value[VAL_SIZE];
        uint8_t type;
        char key[KEY_SIZE];
    };

    namespace RequestTypeRW {
        const uint8_t TYPERW_PUT = 0;
        const uint8_t TYPERW_GET = 1;
        const uint8_t TYPERW_OK = 0;
        const uint8_t TYPERW_EEXIST = -1;
    };
    struct RequestResponseRW {
        uint8_t type;
        char key[KEY_SIZE];
        uint64_t foffset;
    };

    class MailBox {
    public:
        MailBox();

        MailBox(std::string &address); // only for recving socket
        MailBox(int fdesc);

        MailBox &operator=(int fd);

        int open(); // only for sending socket
        ssize_t getOne(char *buf, size_t len, struct sockaddr_un *pairAddress);

        ssize_t sendOne(const char *buf, size_t len, const struct sockaddr_un *pairAddress);

        int close();

        int desc;
    };

    class Multiplexer {
    public:
        Multiplexer();

        int open();

        int listen(const MailBox &mailbox); // listen for receiving
        int unlisten(const MailBox &mailbox); // unlisten
        int wait(MailBox *succeed, int maxevs, int timeout); // unit ms
        int close();

        int desc;
    };
};


#endif
