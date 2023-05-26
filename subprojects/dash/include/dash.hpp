// Copyright (c) Simon Fraser University & The Chinese University of Hong Kong. All rights reserved.
// Licensed under the MIT license.
//
// Dash Extendible Hashing
// Authors:
// Baotong Lu <btlu@cse.cuhk.edu.hk>
// Xiangpeng Hao <xiangpeng_hao@sfu.ca>
// Tianzheng Wang <tzwang@sfu.ca>

#pragma once

#include <immintrin.h>
#include <omp.h>

#include <bitset>
#include <cassert>
#include <cmath>
#include <cstring>
#include <iostream>
#include <shared_mutex>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <vector>

#if defined PMHB_LATENCY || defined COUNTING_WRITE
#include "../../include/sample_guard.hpp"
#endif
#include "substructure.hpp"

#ifdef PMEM
#include <libpmemobj.h>
#endif

// #define DASH_UPDATE_DEBUG

namespace dash_ns {


template<class KV>
class Finger_EH {
public:
    Finger_EH(void);
    Finger_EH(size_t, PMEMobjpool *_pool);
    ~Finger_EH(void);
    inline int
    Insert(std::string_view key, std::string_view value,
           c_ptr<typename KV::K_TYPE> k_ptr = c_ptr<typename KV::K_TYPE>(0ul),
           c_ptr<typename KV::V_TYPE> v_ptr = c_ptr<typename KV::V_TYPE>(0ul));
    inline bool Delete(std::string_view);
    bool Delete(KV, bool);
    inline Value_t Get(std::string_view);
    inline int
    Update(std::string_view key, std::string_view value,
           c_ptr<typename KV::K_TYPE> k_ptr = c_ptr<typename KV::K_TYPE>(0ul),
           c_ptr<typename KV::V_TYPE> v_ptr = c_ptr<typename KV::V_TYPE>(0ul));
    void Directory_Doubling(int x, Table<KV> *new_b, Table<KV> *old_b);
    void Directory_Update(Directory<KV> *_sa, int x, Table<KV> *new_b,
                          Table<KV> *old_b);
    void ShutDown() {
        clean = true;
        Allocator::Persist<KV>(&clean, sizeof(clean));
    }
    void getNumber() {
        // std::cout << "The size of the bucket is " << sizeof(struct Bucket<KV>)
        //           << std::endl;
        size_t _count = 0;
        size_t seg_count = 0;
        Directory<KV> *seg = dir;
        Table<KV> **dir_entry = seg->_;
        Table<KV> *ss;
        auto global_depth = seg->global_depth;
        size_t depth_diff;
        int capacity = pow(2, global_depth);
        for (int i = 0; i < capacity;) {
            ss = reinterpret_cast<Table<KV> *>(
                    reinterpret_cast<uint64_t>(dir_entry[i]) & tailMask);
            depth_diff = global_depth - ss->local_depth;
            _count += ss->number;
            seg_count++;
            i += pow(2, depth_diff);
        }

        ss = reinterpret_cast<Table<KV> *>(
                reinterpret_cast<uint64_t>(dir_entry[0]) & tailMask);
        uint64_t verify_seg_count = 1;
        while (!OID_IS_NULL(ss->next)) {
            verify_seg_count++;
            ss = reinterpret_cast<Table<KV> *>(pmemobj_direct(ss->next));
        }
        // std::cout << "seg_count = " << seg_count << std::endl;
        // std::cout << "verify_seg_count = " << verify_seg_count << std::endl;
        // // #ifdef COUNTING
        // std::cout << "#items = " << _count << std::endl;
        // std::cout << "load_factor = "
        //           << (double) _count /
        //                      (seg_count * kNumPairPerBucket * (kNumBucket + 2))
        //           << std::endl;
        // std::cout << "Raw_Space: "
        //           << (double) (_count * 16) / (seg_count * sizeof(Table<KV>))
        //           << std::endl;
        std::cout << "load_factor: "
                  << ((double) 0.875 * _count /
                      (seg_count * kNumPairPerBucket * (kNumBucket + 2)))
                  << std::endl;
        // #endif
    }

    void recoverTable(Table<KV> **target_table, size_t, size_t,
                      Directory<KV> *);
    void Recovery(PMEMobjpool *_pop);

    inline int Test_Directory_Lock_Set(void) {
        uint32_t v = __atomic_load_n(&lock, __ATOMIC_ACQUIRE);
        return v & lockSet;
    }

    inline bool try_get_directory_read_lock() {
        uint32_t v = __atomic_load_n(&lock, __ATOMIC_ACQUIRE);
        uint32_t old_value = v & lockMask;
        auto new_value = ((v & lockMask) + 1) & lockMask;
        return DASH_CAS(&lock, &old_value, new_value);
    }

    inline void release_directory_read_lock() { SUB(&lock, 1); }

    void Lock_Directory() {
        uint32_t v = __atomic_load_n(&lock, __ATOMIC_ACQUIRE);
        uint32_t old_value = v & lockMask;
        uint32_t new_value = old_value | lockSet;

        while (!DASH_CAS(&lock, &old_value, new_value)) {
            old_value = old_value & lockMask;
            new_value = old_value | lockSet;
        }

        //wait until the readers all exit the critical section
        v = __atomic_load_n(&lock, __ATOMIC_ACQUIRE);
        while (v & lockMask) { v = __atomic_load_n(&lock, __ATOMIC_ACQUIRE); }
    }

    // just set the lock as 0
    void Unlock_Directory() { __atomic_store_n(&lock, 0, __ATOMIC_RELEASE); }

    size_t get_memory_usage();

    Directory<KV> *dir;
    uint32_t
            lock;// the MSB is the lock bit; remaining bits are used as the counter
    uint64_t
            crash_version; /*when the crash version equals to 0Xff => set the crash
                        version as 0, set the version of all entries as 1*/
    bool clean;
    PMEMobjpool *pool_addr;
    /* directory allocation will write to here first,
   * in oder to perform safe directory allocation
   * */
    PMEMoid back_dir;
};

template<class KV>
Finger_EH<KV>::Finger_EH(size_t initCap, PMEMobjpool *_pool) {
    pool_addr = _pool;
    Directory<KV>::New(&back_dir, initCap, 0);
    dir = reinterpret_cast<Directory<KV> *>(pmemobj_direct(back_dir));
    back_dir = OID_NULL;
    lock = 0;
    crash_version = 0;
    clean = false;
    PMEMoid ptr;

    /*FIXME: make the process of initialization crash consistent*/
    Table<KV>::New(&ptr, dir->global_depth, OID_NULL);
    dir->_[initCap - 1] = (Table<KV> *) pmemobj_direct(ptr);
    dir->_[initCap - 1]->pattern = initCap - 1;
    dir->_[initCap - 1]->state = 0;
    /* Initilize the Directory*/
    for (int i = initCap - 2; i >= 0; --i) {
        Table<KV>::New(&ptr, dir->global_depth, ptr);
        dir->_[i] = (Table<KV> *) pmemobj_direct(ptr);
        dir->_[i]->pattern = i;
        dir->_[i]->state = 0;
    }
    dir->depth_count = initCap;
    c_ptr<typename KV::K_TYPE>::pool_uuid_lo = pmemobj_oid(_pool).pool_uuid_lo;
    c_ptr<typename KV::V_TYPE>::pool_uuid_lo = pmemobj_oid(_pool).pool_uuid_lo;
}

template<class KV>
Finger_EH<KV>::Finger_EH() {
    std::cout << "Reinitialize up" << std::endl;
}

template<class KV>
Finger_EH<KV>::~Finger_EH(void) {
    // TO-DO
}

template<class KV>
void Finger_EH<KV>::Directory_Doubling(int x, Table<KV> *new_b,
                                       Table<KV> *old_b) {
#ifdef PMHB_LATENCY
    auto g = pmhb_ns::sample_guard<Finger_EH<KV>, pmhb_ns::DOUBLE>{};
#endif

    Table<KV> **d = dir->_;
    auto global_depth = dir->global_depth;
    std::cout << "Directory_Doubling towards " << global_depth + 1 << std::endl;
    // if (global_depth + 1 == 10) {
    //     std::cout << "break point" << std::endl;
    //     exit(0);
    // }

    auto capacity = pow(2, global_depth);
    Directory<KV>::New(&back_dir, 2 * capacity, dir->version + 1);
    Directory<KV> *new_sa =
            reinterpret_cast<Directory<KV> *>(pmemobj_direct(back_dir));
    auto dd = new_sa->_;

    for (unsigned i = 0; i < capacity; ++i) {
        dd[2 * i] = d[i];
        dd[2 * i + 1] = d[i];
    }
    dd[2 * x + 1] = reinterpret_cast<Table<KV> *>(
            reinterpret_cast<uint64_t>(new_b) | crash_version);
    new_sa->depth_count = 2;

#ifdef PMEM
    Allocator::Persist<KV>(new_sa, sizeof(Directory<KV>) +
                                           sizeof(uint64_t) * 2 * capacity);
    ++merge_time;
    auto old_dir = dir;
    TX_BEGIN(pool_addr) {
        pmemobj_tx_add_range_direct(&dir, sizeof(dir));
        pmemobj_tx_add_range_direct(&back_dir, sizeof(back_dir));
        pmemobj_tx_add_range_direct(&old_b->local_depth,
                                    sizeof(old_b->local_depth));
        old_b->local_depth += 1;
        // Allocator::Free(dir);
        /*Swap the memory addr between new directory and old directory*/
        dir = new_sa;
        back_dir = OID_NULL;
    }
    TX_ONABORT {
        std::cout << "TXN fails during doubling directory" << std::endl;
    }
    TX_END

#else
    dir = new_sa;
#endif
}

template<class KV>
void Finger_EH<KV>::Directory_Update(Directory<KV> *_sa, int x,
                                     Table<KV> *new_b, Table<KV> *old_b) {
    Table<KV> **dir_entry = _sa->_;
    auto global_depth = _sa->global_depth;
    unsigned depth_diff = global_depth - new_b->local_depth;
    if (depth_diff == 0) {
        if (x % 2 == 0) {
            TX_BEGIN(pool_addr) {
                pmemobj_tx_add_range_direct(&dir_entry[x + 1],
                                            sizeof(Table<KV> *));
                pmemobj_tx_add_range_direct(&old_b->local_depth,
                                            sizeof(old_b->local_depth));
                dir_entry[x + 1] = reinterpret_cast<Table<KV> *>(
                        reinterpret_cast<uint64_t>(new_b) | crash_version);
                old_b->local_depth += 1;
            }
            TX_ONABORT { std::cout << "Error for update txn" << std::endl; }
            TX_END
        } else {
            TX_BEGIN(pool_addr) {
                pmemobj_tx_add_range_direct(&dir_entry[x], sizeof(Table<KV> *));
                pmemobj_tx_add_range_direct(&old_b->local_depth,
                                            sizeof(old_b->local_depth));
                dir_entry[x] = reinterpret_cast<Table<KV> *>(
                        reinterpret_cast<uint64_t>(new_b) | crash_version);
                old_b->local_depth += 1;
            }
            TX_ONABORT { std::cout << "Error for update txn" << std::endl; }
            TX_END
        }
#ifdef COUNTING
        __sync_fetch_and_add(&_sa->depth_count, 2);
#endif
    } else {
        int chunk_size = pow(2, global_depth - (new_b->local_depth - 1));
        x = x - (x % chunk_size);
        int base = chunk_size / 2;
        TX_BEGIN(pool_addr) {
            pmemobj_tx_add_range_direct(&dir_entry[x + base],
                                        sizeof(Table<KV> *) * base);
            pmemobj_tx_add_range_direct(&old_b->local_depth,
                                        sizeof(old_b->local_depth));
            for (int i = base - 1; i >= 0; --i) {
                dir_entry[x + base + i] = reinterpret_cast<Table<KV> *>(
                        reinterpret_cast<uint64_t>(new_b) | crash_version);
            }
            old_b->local_depth += 1;
        }
        TX_ONABORT { std::cout << "Error for update txn" << std::endl; }
        TX_END
    }
    // printf("Done!directory update for %d\n", x);
}


template<class KV>
void Finger_EH<KV>::recoverTable(Table<KV> **target_table, size_t key_hash,
                                 size_t x, Directory<KV> *old_sa) {
    /*Set the lockBit to ahieve the mutal exclusion of the recover process*/
    auto dir_entry = old_sa->_;
    uint64_t snapshot = (uint64_t) *target_table;
    Table<KV> *target = (Table<KV> *) (snapshot & tailMask);
    if (pmemobj_mutex_trylock(pool_addr, &target->lock_bit) != 0) { return; }

    target->recoverMetadata();
    if (target->state != 0) {
        target->pattern =
                key_hash >> (8 * sizeof(key_hash) - target->local_depth);
        Allocator::Persist<KV>(&target->pattern, sizeof(target->pattern));
        Table<KV> *next_table = (Table<KV> *) pmemobj_direct(target->next);
        if (target->state == -2) {
            if (next_table->state == -3) {
                /*Help finish the split operation*/
                next_table->recoverMetadata();
                target->HelpSplit(next_table);
                Lock_Directory();
                auto x = (key_hash >>
                          (8 * sizeof(key_hash) - dir->global_depth));
                if (target->local_depth < dir->global_depth) {
                    Directory_Update(dir, x, next_table, target);
                } else {
                    Directory_Doubling(x, next_table, target);
                }
                Unlock_Directory();
                /*release the lock for the target bucket and the new bucket*/
                next_table->state = 0;
                Allocator::Persist<KV>(&next_table->state, sizeof(int));
            }
        } else if (target->state == -1) {
            // if (next_table->pattern == ((target->pattern << 1) + 1)) {
            //     // target->Merge(next_table, true);
            //     Allocator::Persist<KV>(target, sizeof(Table<KV>));
            //     target->next = next_table->next;
            //     Allocator::Free(next_table);
            // }
            // LOG_FATAL("Merge triggered");
        }
        target->state = 0;
        Allocator::Persist<KV>(&target->state, sizeof(int));
    }

    /*Compute for all entries and clear the dirty bit*/
    int chunk_size = pow(2, old_sa->global_depth - target->local_depth);
    x = x - (x % chunk_size);
    for (int i = x; i < (x + chunk_size); ++i) {
        dir_entry[i] = reinterpret_cast<Table<KV> *>(
                (reinterpret_cast<uint64_t>(dir_entry[i]) & tailMask) |
                crash_version);
    }
    *target_table = reinterpret_cast<Table<KV> *>(
            reinterpret_cast<uint64_t>(target) | crash_version);
}

template<class KV>
void Finger_EH<KV>::Recovery(PMEMobjpool *_pop) {
    /*scan the directory, set the clear bit, and also set the dirty bit in the
   * segment to indicate that this segment is clean*/
    c_ptr<typename KV::K_TYPE>::pool_uuid_lo = pmemobj_oid(_pop).pool_uuid_lo;
    c_ptr<typename KV::V_TYPE>::pool_uuid_lo = pmemobj_oid(_pop).pool_uuid_lo;
    if (clean) {
        clean = false;
        return;
    }

    lock = 0;
    /*first check the back_dir log*/
    if (!OID_IS_NULL(back_dir)) { pmemobj_free(&back_dir); }

    auto dir_entry = dir->_;
    int length = pow(2, dir->global_depth);
    crash_version = ((crash_version >> 56) + 1) << 56;
    if (crash_version == 0) {
        uint64_t set_one = 1UL << 56;
        for (int i = 0; i < length; ++i) {
            uint64_t snapshot = (uint64_t) dir_entry[i];
            dir_entry[i] = reinterpret_cast<Table<KV> *>((snapshot & tailMask) |
                                                         set_one);
        }
        Allocator::Persist<KV>(dir_entry, sizeof(uint64_t) * length);
    }
}

template<class KV>
int Finger_EH<KV>::Insert(std::string_view key, std::string_view value,
                          c_ptr<typename KV::K_TYPE> k_ptr,
                          c_ptr<typename KV::V_TYPE> v_ptr) {
#ifdef PMHB_LATENCY
    auto g = pmhb_ns::sample_guard<Finger_EH<KV>, pmhb_ns::INSERT>{};
#endif


    uint64_t key_hash;

    key_hash = h(key.data(), strlen(key.data()));

#ifdef INSERT_DEBUG
    // time_guard tg(fmt::format("Insert {} {:x}, with off {}", key.data(),
    //                           key_hash, k_ptr.offset));
    time_guard tg("Insert ");
#endif
    size_t retry_count_debug = 0;

    // LOG("DASH inserting (" << *key << ", " << value << ") hash " << key_hash);

    auto meta_hash = ((uint8_t) (key_hash & kMask));// the last 8 bits
RETRY:

    // std::cout << "[debug] key " << key.data() << std::endl;

    auto old_sa = dir;
    auto x = (key_hash >> (8 * sizeof(key_hash) - old_sa->global_depth));
    auto dir_entry = old_sa->_;
    Table<KV> *target = reinterpret_cast<Table<KV> *>(
            reinterpret_cast<uint64_t>(dir_entry[x]) & tailMask);

    if ((reinterpret_cast<uint64_t>(dir_entry[x]) & headerMask) !=
        crash_version) {
        recoverTable(&dir_entry[x], key_hash, x, old_sa);
        goto RETRY;
    }
    int ret;

    ret = target->Insert(key, value, key_hash, meta_hash, &dir, k_ptr, v_ptr);


    // std::cout << "[debug] segment insert result " << ret << std::endl;
    // if (retry_count_debug != 0) {
    //     // TODO list the bucket
    //     auto y = BUCKET_INDEX(key_hash);
    //     Bucket<KV> *b = target->bucket + y;
    //     b->list();
    // }
    // retry_count_debug++;


    if (ret == -3) { /*duplicate insert, insertion failure*/
        return -1;
    }

    if (ret == -1) {
        if (!target->bucket->try_get_lock()) { goto RETRY; }

        /*verify procedure*/
        auto old_sa = dir;
        auto x = (key_hash >> (8 * sizeof(key_hash) - old_sa->global_depth));
        if (reinterpret_cast<Table<KV> *>(
                    reinterpret_cast<uint64_t>(old_sa->_[x]) & tailMask) !=
            target) /* verify process*/
        {
            target->bucket->release_lock();
            goto RETRY;
        }
        Table<KV> *new_b;
        {
#ifdef INSERT_DEBUG
            time_guard tg1("split", tg);
#endif

            new_b = target->Split(key_hash);
        }
        /* also needs the verify..., and we use try lock for this rather than the spin lock*/
        /* update directory*/
    REINSERT:
        old_sa = dir;
        dir_entry = old_sa->_;
        x = (key_hash >> (8 * sizeof(key_hash) - old_sa->global_depth));
        if (target->local_depth < old_sa->global_depth) {
            if (!try_get_directory_read_lock()) { goto REINSERT; }

            if (old_sa->version != dir->version) {
                // The directory has changed, thus need retry this update
                release_directory_read_lock();
                goto REINSERT;
            }

            Directory_Update(old_sa, x, new_b, target);
            release_directory_read_lock();
        } else {
            Lock_Directory();
            if (old_sa->version != dir->version) {
                Unlock_Directory();
                goto REINSERT;
            }
#ifdef INSERT_DEBUG
            time_guard tg1("double", tg);
#endif

            Directory_Doubling(x, new_b, target);
            Unlock_Directory();
        }

        /*release the lock for the target bucket and the new bucket*/
        new_b->state = 0;
        Allocator::Persist<KV>(&new_b->state, sizeof(int));
        target->state = 0;
        Allocator::Persist<KV>(&target->state, sizeof(int));

        Bucket<KV> *curr_bucket;
        for (int i = 0; i < kNumBucket; ++i) {
            curr_bucket = target->bucket + i;
            curr_bucket->release_lock();
        }
        curr_bucket = new_b->bucket;
        curr_bucket->release_lock();
        goto RETRY;
    } else if (ret == -2) {
        goto RETRY;
    }

    return 0;
}

template<class KV>
Value_t Finger_EH<KV>::Get(std::string_view key) {
#ifdef PMHB_LATENCY
    auto g = pmhb_ns::sample_guard<Finger_EH<KV>, pmhb_ns::SEARCH>{};
#endif

    uint64_t key_hash;

    key_hash = h(key.data(), strlen(key.data()));

    auto meta_hash = ((uint8_t) (key_hash & kMask));// the last 8 bits
RETRY:
    auto old_sa = dir;
    auto x = (key_hash >> (8 * sizeof(key_hash) - old_sa->global_depth));
    auto y = BUCKET_INDEX(key_hash);
    auto dir_entry = old_sa->_;
    auto old_entry = dir_entry[x];
    Table<KV> *target = reinterpret_cast<Table<KV> *>(
            reinterpret_cast<uint64_t>(old_entry) & tailMask);

    if ((reinterpret_cast<uint64_t>(old_entry) & headerMask) != crash_version) {
        recoverTable(&dir_entry[x], key_hash, x, old_sa);
        goto RETRY;
    }

    Bucket<KV> *target_bucket = target->bucket + y;
    Bucket<KV> *neighbor_bucket = target->bucket + ((y + 1) & bucketMask);

    uint32_t old_version =
            __atomic_load_n(&target_bucket->version_lock, __ATOMIC_ACQUIRE);
    uint32_t old_neighbor_version =
            __atomic_load_n(&neighbor_bucket->version_lock, __ATOMIC_ACQUIRE);

    if ((old_version & lockSet) || (old_neighbor_version & lockSet)) {
        goto RETRY;
    }

    /*verification procedure*/
    old_sa = dir;
    x = (key_hash >> (8 * sizeof(key_hash) - old_sa->global_depth));
    if (old_sa->_[x] != old_entry) { goto RETRY; }

    auto ret = target_bucket->check_and_get(meta_hash, key, false);
    if (target_bucket->test_lock_version_change(old_version)) { goto RETRY; }

#ifdef DASH_UPDATE_DEBUG
    if (key == "627883403595157" && ret != NONE) {
        fmt::print("Found 627883403595157 in target_bucket\n");
    }
#endif

    if (ret != NONE) { return ret; }

    ret = neighbor_bucket->check_and_get(meta_hash, key, true);
    if (neighbor_bucket->test_lock_version_change(old_neighbor_version)) {
        goto RETRY;
    }

#ifdef DASH_UPDATE_DEBUG
    if (key == "627883403595157" && ret != NONE) {
        fmt::print("Found 627883403595157 in neighbor\n");
    }
#endif

    if (ret != NONE) { return ret; }

    if (target_bucket->test_stash_check()) {
        auto test_stash = false;
        if (target_bucket->test_overflow()) {
            /*this only occur when the bucket has more key-values than 10 that are
       * overfloed int he shared bucket area, therefore it needs to search in
       * the extra bucket*/
            test_stash = true;
        } else {
            /*search in the original bucket*/
            int mask = target_bucket->overflowBitmap & overflowBitmapMask;
            if (mask != 0) {
                for (int i = 0; i < 4; ++i) {
                    if (CHECK_BIT(mask, i) &&
                        (target_bucket->finger_array[14 + i] == meta_hash) &&
                        (((1 << i) & target_bucket->overflowMember) == 0)) {
                        Bucket<KV> *stash =
                                target->bucket + kNumBucket +
                                ((target_bucket->overflowIndex >> (i * 2)) &
                                 stashMask);
                        auto ret = stash->check_and_get(meta_hash, key, false);
                        if (ret != NONE) {
                            if (target_bucket->test_lock_version_change(
                                        old_version)) {
                                goto RETRY;
                            }

#ifdef DASH_UPDATE_DEBUG
                            if (key == "627883403595157" && ret != NONE) {
                                fmt::print("Found 627883403595157 in "
                                           "stash\n");
                            }
#endif

                            return ret;
                        }
                    }
                }
            }

            mask = neighbor_bucket->overflowBitmap & overflowBitmapMask;
            if (mask != 0) {
                for (int i = 0; i < 4; ++i) {
                    if (CHECK_BIT(mask, i) &&
                        (neighbor_bucket->finger_array[14 + i] == meta_hash) &&
                        (((1 << i) & neighbor_bucket->overflowMember) != 0)) {
                        Bucket<KV> *stash =
                                target->bucket + kNumBucket +
                                ((neighbor_bucket->overflowIndex >> (i * 2)) &
                                 stashMask);
                        auto ret = stash->check_and_get(meta_hash, key, false);
                        if (ret != NONE) {
                            if (target_bucket->test_lock_version_change(
                                        old_version)) {
                                goto RETRY;
                            }
#ifdef DASH_UPDATE_DEBUG
                            if (key == "627883403595157" && ret != NONE) {
                                fmt::print("Found 627883403595157 in "
                                           "neighbor stash\n");
                            }
#endif

                            return ret;
                        }
                    }
                }
            }
            goto FINAL;
        }
    TEST_STASH:
        if (test_stash == true) {
            for (int i = 0; i < stashBucket; ++i) {
                Bucket<KV> *stash = target->bucket + kNumBucket +
                                    ((i + (y & stashMask)) & stashMask);
                auto ret = stash->check_and_get(meta_hash, key, false);
                if (ret != NONE) {
                    if (target_bucket->test_lock_version_change(old_version)) {
                        goto RETRY;
                    }

#ifdef DASH_UPDATE_DEBUG
                    if (key == "627883403595157" && ret != NONE) {
                        fmt::print(
                                "Found 627883403595157 in TEST_STASH stash\n");
                    }
#endif

                    return ret;
                }
            }
        }
    }
FINAL:
    return NONE;
}


/*By default, the merge operation is disabled*/
template<class KV>
int Finger_EH<KV>::Update(std::string_view key, std::string_view value,
                          c_ptr<typename KV::K_TYPE> k_ptr,
                          c_ptr<typename KV::V_TYPE> v_ptr) {
#ifdef PMHB_LATENCY
    auto g = pmhb_ns::sample_guard<Finger_EH<KV>, pmhb_ns::UPDATE>{};
#endif

    uint64_t key_hash;

    key_hash = h(key.data(), strlen(key.data()));

    auto meta_hash = ((uint8_t) (key_hash & kMask));// the last 8 bits
RETRY:
    auto old_sa = dir;
    auto x = (key_hash >> (8 * sizeof(key_hash) - old_sa->global_depth));
    auto dir_entry = old_sa->_;
    Table<KV> *target_table = reinterpret_cast<Table<KV> *>(
            reinterpret_cast<uint64_t>(dir_entry[x]) & tailMask);

    if ((reinterpret_cast<uint64_t>(dir_entry[x]) & headerMask) !=
        crash_version) {
        recoverTable(&dir_entry[x], key_hash, x, old_sa);
        goto RETRY;
    }

    /*we need to first do the locking and then do the verify*/
    auto y = BUCKET_INDEX(key_hash);
    Bucket<KV> *target = target_table->bucket + y;
    Bucket<KV> *neighbor = target_table->bucket + ((y + 1) & bucketMask);
    target->get_lock();
    if (!neighbor->try_get_lock()) {
        target->release_lock();
        goto RETRY;
    }

    old_sa = dir;
    x = (key_hash >> (8 * sizeof(key_hash) - old_sa->global_depth));
    if (reinterpret_cast<Table<KV> *>(reinterpret_cast<uint64_t>(old_sa->_[x]) &
                                      tailMask) != target_table) {
        target->release_lock();
        neighbor->release_lock();
        goto RETRY;
    }

    auto ret = target->Update(meta_hash, false, key, k_ptr, v_ptr);
    if (ret == 0) {
#ifdef COUNTING
        auto num = SUB(&target_table->number, 1);
#endif
        target->release_lock();
        neighbor->release_lock();
#ifdef COUNTING
        // if (num == 0) { TryMerge(key_hash); }
#endif
        return true;
    }

    ret = neighbor->Update(meta_hash, true, key, k_ptr, v_ptr);
    if (ret == 0) {
#ifdef COUNTING
        auto num = SUB(&target_table->number, 1);
#endif
        neighbor->release_lock();
        target->release_lock();
#ifdef COUNTING
        // if (num == 0) { TryMerge(key_hash); }
#endif
        return true;
    }

    if (target->test_stash_check()) {
        auto test_stash = false;
        if (target->test_overflow()) {
            /*this only occur when the bucket has more key-values than 10 that are
       * overfloed int he shared bucket area, therefore it needs to search in
       * the extra bucket*/
            test_stash = true;
        } else {
            /*search in the original bucket*/
            int mask = target->overflowBitmap & overflowBitmapMask;
            if (mask != 0) {
                for (int i = 0; i < 4; ++i) {
                    if (CHECK_BIT(mask, i) &&
                        (target->finger_array[14 + i] == meta_hash) &&
                        (((1 << i) & target->overflowMember) == 0)) {
                        test_stash = true;
                        goto TEST_STASH;
                    }
                }
            }

            mask = neighbor->overflowBitmap & overflowBitmapMask;
            if (mask != 0) {
                for (int i = 0; i < 4; ++i) {
                    if (CHECK_BIT(mask, i) &&
                        (neighbor->finger_array[14 + i] == meta_hash) &&
                        (((1 << i) & neighbor->overflowMember) != 0)) {
                        test_stash = true;
                        break;
                    }
                }
            }
        }

    TEST_STASH:
        if (test_stash == true) {
            Bucket<KV> *stash = target_table->bucket + kNumBucket;
            stash->get_lock();
            for (int i = 0; i < stashBucket; ++i) {
                int index = ((i + (y & stashMask)) & stashMask);
                Bucket<KV> *curr_stash =
                        target_table->bucket + kNumBucket + index;
                auto ret =
                        curr_stash->Update(meta_hash, false, key, k_ptr, v_ptr);
                if (ret == 0) {
                    /*need to unset indicator in original bucket*/
                    stash->release_lock();
                    neighbor->release_lock();
                    target->release_lock();
                    return true;
                }
            }
            stash->release_lock();
        }
    }
    neighbor->release_lock();
    target->release_lock();
    return false;
}


/*By default, the merge operation is disabled*/
template<class KV>
bool Finger_EH<KV>::Delete(std::string_view key) {
#ifdef PMHB_LATENCY
    auto g = pmhb_ns::sample_guard<Finger_EH<KV>, pmhb_ns::DELETE>{};
#endif

    /*Basic delete operation and merge operation*/
    uint64_t key_hash;

    key_hash = h(key.data(), strlen(key.data()));

    auto meta_hash = ((uint8_t) (key_hash & kMask));// the last 8 bits
RETRY:
    auto old_sa = dir;
    auto x = (key_hash >> (8 * sizeof(key_hash) - old_sa->global_depth));
    auto dir_entry = old_sa->_;
    Table<KV> *target_table = reinterpret_cast<Table<KV> *>(
            reinterpret_cast<uint64_t>(dir_entry[x]) & tailMask);

    if ((reinterpret_cast<uint64_t>(dir_entry[x]) & headerMask) !=
        crash_version) {
        recoverTable(&dir_entry[x], key_hash, x, old_sa);
        goto RETRY;
    }

    /*we need to first do the locking and then do the verify*/
    auto y = BUCKET_INDEX(key_hash);
    Bucket<KV> *target = target_table->bucket + y;
    Bucket<KV> *neighbor = target_table->bucket + ((y + 1) & bucketMask);
    target->get_lock();
    if (!neighbor->try_get_lock()) {
        target->release_lock();
        goto RETRY;
    }

    old_sa = dir;
    x = (key_hash >> (8 * sizeof(key_hash) - old_sa->global_depth));
    if (reinterpret_cast<Table<KV> *>(reinterpret_cast<uint64_t>(old_sa->_[x]) &
                                      tailMask) != target_table) {
        target->release_lock();
        neighbor->release_lock();
        goto RETRY;
    }

    auto ret = target->Delete(key, meta_hash, false);
    if (ret == 0) {
#ifdef COUNTING
        auto num = SUB(&target_table->number, 1);
#endif
        target->release_lock();
#ifdef PMEM
        Allocator::Persist<KV>(&target->bitmap, sizeof(target->bitmap));
#endif
        neighbor->release_lock();
#ifdef COUNTING
        // if (num == 0) { TryMerge(key_hash); }
#endif
        return true;
    }

    ret = neighbor->Delete(key, meta_hash, true);
    if (ret == 0) {
#ifdef COUNTING
        auto num = SUB(&target_table->number, 1);
#endif
        neighbor->release_lock();
#ifdef PMEM
        Allocator::Persist<KV>(&neighbor->bitmap, sizeof(neighbor->bitmap));
#endif
        target->release_lock();
#ifdef COUNTING
        // if (num == 0) { TryMerge(key_hash); }
#endif
        return true;
    }

    if (target->test_stash_check()) {
        auto test_stash = false;
        if (target->test_overflow()) {
            /*this only occur when the bucket has more key-values than 10 that are
       * overfloed int he shared bucket area, therefore it needs to search in
       * the extra bucket*/
            test_stash = true;
        } else {
            /*search in the original bucket*/
            int mask = target->overflowBitmap & overflowBitmapMask;
            if (mask != 0) {
                for (int i = 0; i < 4; ++i) {
                    if (CHECK_BIT(mask, i) &&
                        (target->finger_array[14 + i] == meta_hash) &&
                        (((1 << i) & target->overflowMember) == 0)) {
                        test_stash = true;
                        goto TEST_STASH;
                    }
                }
            }

            mask = neighbor->overflowBitmap & overflowBitmapMask;
            if (mask != 0) {
                for (int i = 0; i < 4; ++i) {
                    if (CHECK_BIT(mask, i) &&
                        (neighbor->finger_array[14 + i] == meta_hash) &&
                        (((1 << i) & neighbor->overflowMember) != 0)) {
                        test_stash = true;
                        break;
                    }
                }
            }
        }

    TEST_STASH:
        if (test_stash == true) {
            Bucket<KV> *stash = target_table->bucket + kNumBucket;
            stash->get_lock();
            for (int i = 0; i < stashBucket; ++i) {
                int index = ((i + (y & stashMask)) & stashMask);
                Bucket<KV> *curr_stash =
                        target_table->bucket + kNumBucket + index;
                auto ret = curr_stash->Delete(key, meta_hash, false);
                if (ret == 0) {
                    /*need to unset indicator in original bucket*/
                    stash->release_lock();
#ifdef PMEM
                    Allocator::Persist<KV>(&curr_stash->bitmap,
                                           sizeof(curr_stash->bitmap));
#endif
                    auto bucket_ix = BUCKET_INDEX(key_hash);
                    auto org_bucket = target_table->bucket + bucket_ix;
                    assert(org_bucket == target);
                    target->unset_indicator(meta_hash, neighbor, index);
#ifdef COUNTING
                    auto num = SUB(&target_table->number, 1);
#endif
                    neighbor->release_lock();
                    target->release_lock();
#ifdef COUNTING
                    // if (num == 0) { TryMerge(key_hash); }
#endif
                    return true;
                }
            }
            stash->release_lock();
        }
    }
    neighbor->release_lock();
    target->release_lock();
    return false;
}

template<class KV>
size_t Finger_EH<KV>::get_memory_usage() {
    /* traverse all structure */

    size_t sum = 0;

    const auto &d = *dir;
    size_t capacity = 1ul << dir->global_depth;
    Table<KV> *last_pointer = nullptr;

    for (size_t i = 0; i < capacity; i++) {
        if (last_pointer - d._[i] == 0) {
            continue;
        } else {
            sum += sizeof(Table<KV>);
            last_pointer = d._[i];
        }
    }
    /* Directory */
    sum += sizeof(Table<KV> *) * capacity + sizeof(Directory<KV>);
    return sum;
}

/*DEBUG FUNCTION: search the position of the key in this table and print
 * correspongdign informantion in this table, to test whether it is correct*/

#undef BUCKET_INDEX
#undef GET_COUNT
#undef GET_BITMAP
#undef GET_MEMBER
#undef GET_INVERSE_MEMBER
}// namespace dash_ns
