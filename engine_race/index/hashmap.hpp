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
#include <random>
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

namespace polar_race {

    extern std::mt19937 eng;
    extern std::uniform_int_distribution<uint32_t> uid;

    template<typename T>
    constexpr T constexpr_log2_(T a, T e) {
        return e == T(1) ? a : constexpr_log2_(a + T(1), e / T(2));
    }

    template<typename T>
    constexpr std::size_t constexpr_find_last_set(T const t) {
        return t == T(0) ? 0 : 1 + constexpr_log2_(T(0), t);
    }


    template<class T>
    constexpr T constexpr_next_pow_two(T const v) {
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

    struct MutableData {
        uint32_t data;

        MutableData(uint32_t init) : data(init) {}
    };

    class MMapAlloc {
    public:
        size_t computeSize(size_t size) {
            long pagesize = sysconf(_SC_PAGESIZE);
            size_t mmapLength = ((size - 1) & ~(pagesize - 1)) + pagesize;
            assert(size <= mmapLength && mmapLength < size + pagesize);
            assert((mmapLength % pagesize) == 0);
            return mmapLength;
        }

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

    struct AtomicUnorderedInsertMap {
        typedef struct ConstIterator {
            ConstIterator(const AtomicUnorderedInsertMap &owner, uint32_t slot)
                    : owner_(owner), slot_(slot) {}

            ConstIterator(const ConstIterator &) = default;

            ConstIterator &operator=(const ConstIterator &) = default;

            std::pair<uint64_t, MutableData> &operator*() const {
                return owner_.slots_[slot_].keyValue();
            }

            std::pair<uint64_t, MutableData> *operator->() const {
                return &owner_.slots_[slot_].keyValue();
            }

            const ConstIterator &operator++() {
                while (slot_ > 0) {
                    --slot_;
                    if (owner_.slots_[slot_].state() == LINKED) {
                        break;
                    }
                }
                return *this;
            }

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
            uint32_t slot_;
        } const_iterator;

        friend ConstIterator;

        explicit AtomicUnorderedInsertMap(
                size_t maxSize,
                float maxLoadFactor = 0.8f,
                const MMapAlloc &alloc = MMapAlloc())
                : allocator_(alloc) {
            size_t capacity = size_t(maxSize / std::min(1.0f, maxLoadFactor) + 128);
            size_t avail = size_t{1} << (8 * sizeof(uint32_t) - 2);
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
            slotMask_ = constexpr_next_pow_two(capacity * 4) - 1;
            mmapRequested_ = sizeof(Slot) * capacity;
            slots_ = reinterpret_cast<Slot *>(allocator_.allocate(mmapRequested_));
            slots_[0].stateUpdate(EMPTY, CONSTRUCTING);

        }

        ~AtomicUnorderedInsertMap() {
            for (size_t i = 1; i < numSlots_; ++i) {
                slots_[i].~Slot();
            }
            allocator_.deallocate(reinterpret_cast<char *>(slots_), mmapRequested_);
        }

        template<typename Func>
        std::pair<const_iterator, bool> findOrConstruct(const uint64_t &key, Func &&func) {
            auto const slot = keyToSlotIdx(key);
            auto prev = slots_[slot].headAndState_.load(std::memory_order_acquire);

            auto existing = find(key, slot);
            if (existing != 0) {
                return std::make_pair(ConstIterator(*this, existing), false);
            }

            auto idx = allocateNear(slot);
            new(&slots_[idx].keyValue().first) uint64_t(key);
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
                    slots_[idx].stateUpdate(CONSTRUCTING, EMPTY);

                    return std::make_pair(ConstIterator(*this, existing), false);
                }
            }
        }

        template<class K, class V>
        std::pair<const_iterator, bool> emplace(const K &key, V &&value) {
            return findOrConstruct(
                    key, [&](void *raw) { new(raw) MutableData(std::forward<V>(value)); });
        }

        const_iterator find(const uint64_t &key) const {
            return ConstIterator(*this, find(key, keyToSlotIdx(key)));
        }

        const_iterator cbegin() const {
            uint32_t slot = numSlots_ - 1;
            while (slot > 0 && slots_[slot].state() != LINKED) {
                --slot;
            }
            return ConstIterator(*this, slot);
        }

        const_iterator cend() const {
            return ConstIterator(*this, 0);
        }

        enum : uint32_t {
            kMaxAllocationTries = 1000, // after this we throw
        };

        enum BucketState : uint32_t {
            EMPTY = 0,
            CONSTRUCTING = 1,
            LINKED = 2,
        };

        struct Slot {
            std::atomic<uint32_t> headAndState_;
            uint32_t next_;
            std::pair<uint64_t, MutableData> raw_;

            BucketState state() const {
                return BucketState(headAndState_.load(std::memory_order_acquire) & 3);
            }

            void stateUpdate(BucketState before, BucketState after) {
                assert(state() == before);
                headAndState_ += (after - before);
            }

            std::pair<uint64_t, MutableData> &keyValue() {
                assert(state() != EMPTY);
                return *static_cast<std::pair<uint64_t, MutableData> *>(static_cast<void *>(&raw_));
            }

            const std::pair<uint64_t, MutableData> &keyValue() const {
                assert(state() != EMPTY);
                return *static_cast<const std::pair<uint64_t, MutableData> *>(static_cast<const void *>(&raw_));
            }

            bool operator<(const Slot &another) const {
                return this->raw_.first > another.raw_.first;
//                int answer = memcmp(&(this->raw_.first), &(another.raw_.first), 8);
//                return answer >= 0;
            }

            Slot(const Slot &another) : raw_(another.raw_) {
                headAndState_ = another.headAndState_.load(std::memory_order_relaxed);
                next_ = another.next_;
            }

            Slot &operator=(const Slot &another) {
                headAndState_ = another.headAndState_.load(std::memory_order_relaxed);
                next_ = another.next_;
                raw_ = another.raw_;
                return *this;
            }


        };

        size_t mmapRequested_;
        size_t numSlots_;

        size_t slotMask_;
        MMapAlloc allocator_;
        Slot *slots_;

        uint32_t keyToSlotIdx(const uint64_t &key) const {
            size_t h = std::hash<uint64_t>()(key);
            h &= slotMask_;
            while (h >= numSlots_) {
                h -= numSlots_;
            }
            return h;
        }

        uint32_t find(const uint64_t &key, uint32_t slot) const {
            std::equal_to<uint64_t> ke = {};
            auto hs = slots_[slot].headAndState_.load(std::memory_order_acquire);
            for (slot = hs >> 2; slot != 0; slot = slots_[slot].next_) {
                if (ke(key, slots_[slot].keyValue().first)) {
                    return slot;
                }
            }
            return 0;
        }

        uint32_t allocateNear(uint32_t start) {
            for (uint32_t tries = 0; tries < kMaxAllocationTries; ++tries) {
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

        uint32_t allocationAttempt(uint32_t start, uint32_t tries) const {
            if (LIKELY(tries < 8 && start + tries < numSlots_)) {
                return uint32_t(start + tries);
            } else {
                uint32_t rv = uint32_t(uid(eng));
                assert(rv < numSlots_);
                return rv;
            }
        }
    };


}

#endif

