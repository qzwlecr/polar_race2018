/*
 * Copyright 2013-present Facebook, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef ENGINE_RACE_INDEX_HASHMAP_H
#define ENGINE_RACE_INDEX_HASHMAP_H

#pragma once

#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <experimental/random>
#include <functional>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <system_error>
#include <type_traits>

#include <unistd.h>
#include <sys/mman.h>

#if defined(__GNUC__)
#define LIKELY(x) (__builtin_expect((x), 1))
#define UNLIKELY(x) (__builtin_expect((x), 0))
#else
#define LIKELY(x) (x)
#define UNLIKELY(x) (x)
#endif

class MMapAlloc {
private:
    size_t computeSize(size_t size) {
        long pagesize = sysconf(_SC_PAGESIZE);
        size_t mmapLength = ((size - 1) & ~(pagesize - 1)) + pagesize;
        assert(size <= mmapLength && mmapLength < size + pagesize);
        assert((mmapLength % pagesize) == 0);
        return mmapLength;
    }

public:
    void *allocate(size_t size) {
        auto len = computeSize(size);

        int extraflags = 0;
#if defined(MAP_POPULATE)
        extraflags |= MAP_POPULATE;
#endif
        void *mem = static_cast<void *>(mmap(
                nullptr,
                len,
                PROT_READ | PROT_WRITE,
                MAP_PRIVATE | MAP_ANONYMOUS | extraflags,
                -1,
                0));
        if (mem == reinterpret_cast<void *>(-1)) {
            throw std::system_error(errno, std::system_category());
        }
#if !defined(MAP_POPULATE) && defined(MADV_WILLNEED)
        madvise(mem, size, MADV_WILLNEED);
#endif

        return mem;
    }

    void deallocate(void *p, size_t size) {
        auto len = computeSize(size);
        munmap(p, len);
    }
};

template<typename Allocator>
struct GivesZeroFilledMemory : public std::false_type {
};

template<>

struct GivesZeroFilledMemory<MMapAlloc> : public std::true_type {
};

template<typename T>
constexpr T constexpr_log2_(T a, T e) {
    return e == T(1) ? a : constexpr_log2_(a + T(1), e / T(2));
}

template<typename T>
constexpr std::size_t constexpr_find_last_set(T const t) {
    return t == T(0) ? 0 : 1 + constexpr_log2_(T(0), t);
}


template<class T>
inline constexpr T nextPowTwo(T const v) {
    return v ? (T(1) << constexpr_find_last_set(v - 1)) : T(1);
}

template<typename T>
constexpr T constexpr_max(T a) {
    return a;
}

template<typename T, typename... Ts>
constexpr T constexpr_max(T a, T b, Ts... ts) {
    return b < a ? constexpr_max(a, ts...) : constexpr_max(b, ts...);
}

template<
        typename Key,
        typename Value,
        typename Hash = std::hash<Key>,
        typename KeyEqual = std::equal_to<Key>,
        template<typename> class Atom = std::atomic,
        typename IndexType = uint32_t,
        typename Allocator = MMapAlloc>

struct AtomicUnorderedInsertMap {
    typedef Key key_type;
    typedef Value mapped_type;
    typedef std::pair<Key, Value> value_type;
    typedef std::size_t size_type;
    typedef std::ptrdiff_t difference_type;
    typedef Hash hasher;
    typedef KeyEqual key_equal;
    typedef const value_type &const_reference;

    typedef struct ConstIterator {
        ConstIterator(const AtomicUnorderedInsertMap &owner, IndexType slot)
                : owner_(owner), slot_(slot) {}

        ConstIterator(const ConstIterator &) = default;

        ConstIterator &operator=(const ConstIterator &) = default;

        const value_type &operator*() const {
            return owner_.slots_[slot_].keyValue();
        }

        const value_type *operator->() const {
            return &owner_.slots_[slot_].keyValue();
        }

        // pre-increment
        const ConstIterator &operator++() {
            while (slot_ > 0) {
                --slot_;
                if (owner_.slots_[slot_].state() == LINKED) {
                    break;
                }
            }
            return *this;
        }

        // post-increment
        ConstIterator operator++(int /* dummy */) {
            auto prev = *this;
            ++*this;
            return prev;
        }

        bool operator==(const ConstIterator &rhs) const {
            return slot_ == rhs.slot_;
        }

        bool operator!=(const ConstIterator &rhs) const {
            return !(*this == rhs);
        }

    private:
        const AtomicUnorderedInsertMap &owner_;
        IndexType slot_;
    } const_iterator;

    friend ConstIterator;

    explicit AtomicUnorderedInsertMap(
            size_t maxSize,
            float maxLoadFactor = 0.8f,
            const Allocator &alloc = Allocator())
            : allocator_(alloc) {
        size_t capacity = size_t(maxSize / std::min(1.0f, maxLoadFactor) + 128);
        size_t avail = size_t{1} << (8 * sizeof(IndexType) - 2);
        if (capacity > avail && maxSize < avail) {
            // we'll do our best
            capacity = avail;
        }
        if (capacity < maxSize || capacity > avail) {
            throw std::invalid_argument(
                    "AtomicUnorderedInsertMap capacity must fit in IndexType with 2 bits "
                    "left over");
        }

        numSlots_ = capacity;
        slotMask_ = nextPowTwo(capacity * 4) - 1;
        mmapRequested_ = sizeof(Slot) * capacity;
        slots_ = reinterpret_cast<Slot *>(allocator_.allocate(mmapRequested_));
        zeroFillSlots();
        slots_[0].stateUpdate(EMPTY, CONSTRUCTING);
    }

    ~AtomicUnorderedInsertMap() {
        for (size_t i = 1; i < numSlots_; ++i) {
            slots_[i].~Slot();
        }
        allocator_.deallocate(reinterpret_cast<char *>(slots_), mmapRequested_);
    }

    template<typename Func>
    std::pair<const_iterator, bool> findOrConstruct(const Key &key, Func &&func) {
        auto const slot = keyToSlotIdx(key);
        auto prev = slots_[slot].headAndState_.load(std::memory_order_acquire);

        auto existing = find(key, slot);
        if (existing != 0) {
            return std::make_pair(ConstIterator(*this, existing), false);
        }

        auto idx = allocateNear(slot);
        new(&slots_[idx].keyValue().first) Key(key);
        func(static_cast<void *>(&slots_[idx].keyValue().second));

        while (true) {
            slots_[idx].next_ = prev >> 2;

            auto after = idx << 2;
            if (slot == idx) {
                after += LINKED;
            } else {
                after += (prev & 3);
            }

            if (slots_[slot].headAndState_.compare_exchange_strong(prev, after)) {
                // success
                if (idx != slot) {
                    slots_[idx].stateUpdate(CONSTRUCTING, LINKED);
                }
                return std::make_pair(ConstIterator(*this, idx), true);
            }

            existing = find(key, slot);
            if (existing != 0) {
                slots_[idx].keyValue().first.~Key();
                slots_[idx].keyValue().second.~Value();
                slots_[idx].stateUpdate(CONSTRUCTING, EMPTY);

                return std::make_pair(ConstIterator(*this, existing), false);
            }
        }
    }

    template<class K, class V>
    std::pair<const_iterator, bool> emplace(const K &key, V &&value) {
        return findOrConstruct(
                key, [&](void *raw) { new(raw) Value(std::forward<V>(value)); });
    }

    const_iterator find(const Key &key) const {
        return ConstIterator(*this, find(key, keyToSlotIdx(key)));
    }

    const_iterator cbegin() const {
        IndexType slot = numSlots_ - 1;
        while (slot > 0 && slots_[slot].state() != LINKED) {
            --slot;
        }
        return ConstIterator(*this, slot);
    }

    const_iterator cend() const {
        return ConstIterator(*this, 0);
    }

    enum : IndexType {
        kMaxAllocationTries = 1000, // after this we throw
    };

    enum BucketState : IndexType {
        EMPTY = 0,
        CONSTRUCTING = 1,
        LINKED = 2,
    };

    struct Slot {
        Atom<IndexType> headAndState_;

        IndexType next_;

        typename std::aligned_storage<sizeof(value_type), alignof(value_type)>::type
                raw_;

        ~Slot() {
            auto s = state();
            assert(s == EMPTY || s == LINKED);
            if (s == LINKED) {
                keyValue().first.~Key();
                keyValue().second.~Value();
            }
        }

        BucketState state() const {
            return BucketState(headAndState_.load(std::memory_order_acquire) & 3);
        }

        void stateUpdate(BucketState before, BucketState after) {
            assert(state() == before);
            headAndState_ += (after - before);
        }

        value_type &keyValue() {
            assert(state() != EMPTY);
            return *static_cast<value_type *>(static_cast<void *>(&raw_));
        }

        const value_type &keyValue() const {
            assert(state() != EMPTY);
            return *static_cast<const value_type *>(static_cast<const void *>(&raw_));
        }
    };

    size_t mmapRequested_;
    size_t numSlots_;

    size_t slotMask_;

    Allocator allocator_;
    Slot *slots_;

    IndexType keyToSlotIdx(const Key &key) const {
        size_t h = hasher()(key);
        h &= slotMask_;
        while (h >= numSlots_) {
            h -= numSlots_;
        }
        return h;
    }

    IndexType find(const Key &key, IndexType slot) const {
        KeyEqual ke = {};
        auto hs = slots_[slot].headAndState_.load(std::memory_order_acquire);
        for (slot = hs >> 2; slot != 0; slot = slots_[slot].next_) {
            if (ke(key, slots_[slot].keyValue().first)) {
                return slot;
            }
        }
        return 0;
    }

    IndexType allocateNear(IndexType start) {
        for (IndexType tries = 0; tries < kMaxAllocationTries; ++tries) {
            auto slot = allocationAttempt(start, tries);
            auto prev = slots_[slot].headAndState_.load(std::memory_order_acquire);
            if ((prev & 3) == EMPTY &&
                slots_[slot].headAndState_.compare_exchange_strong(
                        prev, prev + CONSTRUCTING - EMPTY)) {
                return slot;
            }
        }
        throw std::bad_alloc();
    }

    IndexType allocationAttempt(IndexType start, IndexType tries) const {
        if (LIKELY(tries < 8 && start + tries < numSlots_)) {
            return IndexType(start + tries);
        } else {
            IndexType rv;
            if (sizeof(IndexType) <= 4) {
                rv = IndexType(std::experimental::randint(uint32_t(0), uint32_t(numSlots_)));
            } else {
                rv = IndexType(std::experimental::randint(uint64_t(0), numSlots_));
            }
            assert(rv < numSlots_);
            return rv;
        }
    }

    void zeroFillSlots() {
        if (!GivesZeroFilledMemory<Allocator>::value) {
            memset(slots_, 0, mmapRequested_);
        }
    }
};


template<typename T, template<typename> class Atom = std::atomic>
struct MutableAtom {
    mutable Atom<T> data;

    explicit MutableAtom(const T &init) : data(init) {}
};

#endif
