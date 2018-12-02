#ifndef RALOPDB_RCHACHE_H
#define RALOPDB_RCHACHE_H

#include <list>
#include <atomic>
#include <mutex>
#include <cstdint>
#include "../syscalls/sys.h"

extern "C"{
#include <sys/types.h>
};

namespace polar_race{

class CacheBlock {
    public:
    uint64_t size();
    bool is_here(off_t position);
    bool access(off_t position, void* buffer);
    void load(int fdesc);
    bool is_useless(); // return true on accessed == maxaccessed
    CacheBlock(off_t b, off_t e, uint32_t maxaccess);
    ~CacheBlock();
    private:
    off_t begin;
    off_t end;
    void* cachedata;
    bool loaded;
    std::atomic_uint accessed;
    uint32_t maxaccessed;
};

class RangeCache {
    public:
    bool access(off_t position, void* buffer);
    RangeCache(int backfd, uint32_t mxsize);
    ~RangeCache();
    private:
    bool allocate(uint32_t blkspace);
    std::list<CacheBlock*> blklist;
    RWMutex blistmu;
    int backdesc;
    uint32_t maxsize;
    std::atomic_uint currsize;
};

};


#endif