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

class RangeCacheProfiler{
    public:
    std::atomic<uint64_t> exist_judge_first;
    std::atomic<uint64_t> check_location;
    std::atomic<uint64_t> allocation_first_try;
    std::atomic<uint64_t> real_allocation;
    std::atomic<uint64_t> duplicate_allocation;
    std::atomic<uint64_t> rcache_access;
    std::atomic<uint64_t> rcache_across_free;
    inline RangeCacheProfiler():exist_judge_first(0),check_location(0),
    allocation_first_try(0), real_allocation(0), duplicate_allocation(0),
    rcache_access(0), rcache_across_free(0){};
    void print();
};

class CacheBlock {
    public:
    inline uint64_t size();
    inline bool is_here(off_t position);
    // return 0: failed 1: success 2: you need to free this
    int access(off_t position, void* buffer);
    void load(int fdesc);
    inline uint32_t buck(){return belongs;};
    inline CacheBlock(off_t b, off_t e, uint32_t belongs);
    inline ~CacheBlock();
    off_t begin;
    private:
    off_t end;
    uint8_t* cachedata;
    bool loaded;
    uint32_t belongs;
};

class RangeCache {
    public:
    bool access(off_t position, void* buffer, int32_t& bn, RangeCacheProfiler& rcp);
    void across(uint32_t bn, RangeCacheProfiler& rcp);
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
    std::atomic_uint across_counter[BUCKET_NUMBER];
};


};


#endif