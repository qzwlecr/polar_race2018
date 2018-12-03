#ifndef RALOPDB_RCHACHE_H
#define RALOPDB_RCHACHE_H

#include <list>
#include <atomic>
#include <mutex>
#include <cstdint>
#include "../syscalls/sys.h"
#include "../bucket/bucket_link_list.h"
#include "../consts/consts.h"

extern "C"{
#include <sys/types.h>
};

namespace polar_race{

class CacheBlock {
    public:
    inline uint64_t size();
    inline bool is_here(off_t position);
    // return 0: failed 1: success 2: you need to free this
    int access(off_t position, void* buffer);
    void load(int fdesc);
    inline CacheBlock(off_t b, off_t e, uint32_t maxaccess);
    inline ~CacheBlock();
    off_t begin;
    private:
    off_t end;
    uint8_t* cachedata;
    bool loaded;
    std::atomic_uint accessed;
    uint32_t maxaccessed;
};

class RangeCache {
    public:
    bool access(const char* key, off_t position, void* buffer);
    RangeCache(int metafd, int backfd, uint32_t mxsize);
    ~RangeCache();
    private:
    bool allocate(uint32_t blkspace);
    void deallocate(uint32_t blkspace);
    bool buck_findblk(BucketLinkList* buckptr, off_t position, off_t& fbegin, uint64_t& flength);
    std::list<CacheBlock*>::iterator blklist_exist(off_t position);
    std::list<CacheBlock*> blklist;
    RWMutex blistmu;
    int backfd;
    int metafd;
    uint32_t maxsize;
    std::atomic_uint currsize;
};

};


#endif