#include "rcache.h"

#include "../format/log.h"

#include <cstdlib>
#include <cstring>

extern "C"{
#include <unistd.h>
}

using namespace std;

namespace polar_race{

CacheBlock::CacheBlock(off_t b, off_t e, uint32_t mxaccess){
    cachedata = nullptr;
    begin = b;
    end = e;
    loaded = false;
    accessed = 0;
    maxaccessed = mxaccess;
}

CacheBlock::~CacheBlock(){
    if(cachedata != nullptr) free(cachedata);
}

uint64_t CacheBlock::size(){
    return end - begin;
}

bool CacheBlock::is_here(off_t position){
    return (position >= begin) && (position < end);
}

int CacheBlock::access(off_t position, void* buffer){
    if(!loaded) return 0;
    if(!is_here(position)){
        qLogWarnfmt("CacheBlock[%lu, %lu]: accessing with out-range position: %lu", begin, end, position);
        return 0;
    }
    memcpy(buffer, cachedata + (position - begin), VAL_SIZE);
    if(accessed.fetch_add(1) + 1 >= maxaccessed){
        return 2;
    }
    return 1;
}

void CacheBlock::load(int fdesc){
    if(cachedata == nullptr) cachedata = reinterpret_cast<uint8_t*>(malloc(size()));
    if(cachedata == nullptr){
        qLogFailfmt("CacheBlock[%lu, %lu]: memory allocation failed: is there insufficient memory??", begin, end);
        abort();
    }
    ssize_t rdsz = pread(fdesc, cachedata, size(), begin);
    if(rdsz != size()){
        qLogFailfmt("CacheBlock[%lu, %lu]: cannot read from disk: %s(%ld)", begin, end, strerror(errno), rdsz);
        abort();
    }
}

RangeCache::RangeCache(int mfd, int bfd, uint32_t mxsize):
backfd(bfd), metafd(mfd), maxsize(mxsize), currsize(0){}

RangeCache::~RangeCache(){
    for(auto &x: blklist){
        delete x;
    }
}

bool RangeCache::allocate(uint32_t blkspace){
    uint32_t origsz, newsz;
    do{
        origsz = currsize;
        newsz = origsz + blkspace;
        if(newsz > maxsize) return false;
    }while(!currsize.compare_exchange_weak(origsz, newsz));
    return true;
}

void RangeCache::deallocate(uint32_t blkspace){
    currsize.fetch_sub(blkspace);
    if(currsize < 0){
        qLogFail("RangeCache: Current cache size is less than 0!!");
        qLogFail("RangeCache: This normally indicates critical bugs in cache implementation.");
        qLogFail("RangeCache: Aborting..");
        abort();
    }
}

list<CacheBlock*>::iterator RangeCache::blklist_exist(off_t pos){
    for(auto i = blklist.begin(); i != blklist.end(); i++){
        if((*i)->is_here(pos)){
            return i;
        }
    }
    return blklist.end();
}


bool RangeCache::buck_findblk(BucketLinkList* buckptr, off_t position, off_t &fbegin, uint64_t &flength){
    for(int i = 0; i < buckptr->links.size(); i++){
        fbegin = buckptr->links[i];
        flength = buckptr->sizes[i] * VAL_SIZE;
        if(position >= fbegin && (position - fbegin) < flength){
            return true;
        }
    }
    return false;
}

bool RangeCache::access(const char* key, off_t position, void *buffer){
    blistmu.rdlock();
    auto cbptri = blklist_exist(position);
    auto cbptr = (cbptri == blklist.end() ? nullptr : *cbptri);
    blistmu.unlock();
    if(cbptr == nullptr){
        // cache miss
        qLogDebugfmt("RangeCache: miss at %ld", position);
        // which block we need to load?
        // TODO: find bucket id from provided key.
        int buckid = 0;// INVALID!!
        qLogDebugfmt("RangeCache: going to load from bucket %d", buckid);
        off_t cblkbegin = 0;
        uint64_t cblklen = 0;
        if(!buck_findblk(BucketLinkLists[buckid], position, cblkbegin, cblklen)){
            qLogFailfmt("RangeCache: Cannot find corresponding block in buck %d, from position %lu", buckid, position);
            qLogFail("RangeCache: This normally indicates a critical bug in bucket lookup logic.");
            qLogFail("RangeCache: Aborting..");
            abort();
        }
        qLogDebugfmt("RangeCache: rdfile %ld -> +%ld", cblkbegin, cblklen);
        while(!allocate(cblklen)){
            // do some detection when allocation GG
            blistmu.rdlock();
            cbptri = blklist_exist(position);
            cbptr = (cbptri == blklist.end() ? nullptr : *cbptri);
            blistmu.unlock();
            if(cbptri != blklist.end()) {
                qLogDebug("RangeCache: re-space-allocation detected.");
                break;
            }
        }
        if(cbptr == nullptr){
            // allocation success, no allocated block detected
            blistmu.wrlock();
            cbptri = blklist_exist(position);
            cbptr = (cbptri == blklist.end() ? nullptr : *cbptri);
            if(cbptr == nullptr){
                // then we can do real memory allocation safely
                cbptr = new CacheBlock(cblkbegin, cblkbegin + cblklen, (cblklen / VAL_SIZE) * CONCURRENT_QUERY);
                cbptr->load(backfd);
                qLogDebug("RangeCache: CacheBlock allocated.");
                blklist.push_back(cbptr);
                cbptri = blklist_exist(position);
                blistmu.unlock();
            } else {
                // someone allocated that for me..
                qLogDebug("RangeCache: re-block-allocation detected.");
                blistmu.unlock();
                deallocate(cblklen);
            }
        }
    }
    // complete access
    int acrv = cbptr->access(position, buffer);
    if(acrv == 0){
        qLogFailfmt("RangeCache: Accessing cache position %ld result in failure.", position);
        qLogFail("RangeCache: This normally indicates a critical bug in cache access logic.");
        qLogFail("RangeCache: Aborting..");
        abort();
    }
    // acrv == 1 means it's a normal OK. no special handling needed.
    if(acrv == 2){
        qLogDebugfmt("RangeCache: Block %ld invalidated", (*cbptri)->begin);
        deallocate((*cbptri)->size());
        blistmu.wrlock();
        auto cbptri = blklist_exist(position);
        if(cbptri != blklist.end()){
            delete *cbptri;
            blklist.erase(cbptri);
        } else {
            qLogWarn("RangeCache: re-deallocation detected: please check invalidation log!");
        }
        blistmu.unlock();
    }
    return true;
}


};