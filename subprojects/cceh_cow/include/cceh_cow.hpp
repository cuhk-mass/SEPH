#pragma once
/*
We do several optimization and correctness patches for CCEH_COW, including:
(1) remove fence between storing value and storing key during insert() because
these two stores are in the same cacheline and will mot be reordered. (2) remove
bucket-level lock described in their original paper since frequent
lock/unlocking will severly degrade its performance (actually their original
open-sourced code also does not have bucket-level lock). (3) add epoch manager
in the application level (mini-benchmark) to gurantee correct memory
reclamation. (4) avoid the perssitent memory leak during the segment split by
storing the newly allocated segment in a small preallocated area (organized as a
hash table). (5) add uniqnuess check during the insert opeartion to avoid
inserting duplicate keys. (6) add support for variable-length key by storing the
pointer to the key object. (7) use persistent lock in PMDK library to aovid
deadlock caused by sudden system failure.
*/
#include <bitset>
#include <cassert>
#include <cmath>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <shared_mutex>
#include <thread>
#include <unordered_map>
#include <vector>

#include "allocator.hpp"
#include "hash.hpp"
#include "substructure.hpp"

#include <libpmem.h>
#include <libpmemobj++/pool.hpp>
#include <libpmemobj.h>
#include <libpmempool.h>

#ifdef PMHB_LATENCY
#include "../../include/sample_guard.hpp"
#endif
#define PMEM

#define PERSISTENT_LOCK 1
#define INPLACE 1
#define EPOCH 1
#define LOG_NUM 1024

#define CCEH_COW_CAS(_p, _u, _v)                                               \
    (__atomic_compare_exchange_n(_p, _u, _v, false, __ATOMIC_ACQUIRE,          \
                                 __ATOMIC_ACQUIRE))


namespace cceh_cow_ns {

template<class KV>
struct _Pair {
    c_ptr<typename KV::K_TYPE> key;
    c_ptr<typename KV::V_TYPE> value;
};

struct log_entry {
    uint64_t lock;
    PMEMoid temp;

    void Lock_log() {
        uint64_t temp = 0;
        while (!CCEH_COW_CAS(&lock, &temp, 1)) { temp = 0; }
    }

    void Unlock_log() { lock = 0; }
};

const size_t kCacheLineSize = 64;
constexpr size_t kSegmentBits = 8;
constexpr size_t kMask = (1 << kSegmentBits) - 1;
constexpr size_t kShift = kSegmentBits;
constexpr size_t kSegmentSize = (1 << kSegmentBits) * 16 * 4;
constexpr size_t kNumPairPerCacheLine = kCacheLineSize / 16;
constexpr size_t kNumCacheLine = 4;

uint64_t clflushCount;


template<class KV>
struct Segment {
    static const size_t kNumSlot = kSegmentSize / sizeof(_Pair<KV>);

    Segment(void)
        : local_depth{0}, sema{0}, count{0}, seg_lock{0}, mutex(), rwlock() {
        memset((void *) &_[0], 255, sizeof(_Pair<KV>) * kNumSlot);
    }

    Segment(size_t depth)
        : local_depth{depth}, sema{0}, count{0}, seg_lock{0}, mutex(),
          rwlock() {
        memset((void *) &_[0], 255, sizeof(_Pair<KV>) * kNumSlot);
    }

    static void New(Segment<KV> **seg, size_t depth) {
#ifdef PMEM
        auto callback = [](PMEMobjpool *pool, void *ptr, void *arg) {
            auto value_ptr = reinterpret_cast<size_t *>(arg);
            auto seg_ptr = reinterpret_cast<Segment *>(ptr);
            seg_ptr->local_depth = *value_ptr;
            seg_ptr->sema = 0;
            seg_ptr->count = 0;
            seg_ptr->seg_lock = 0;
            memset((void *) &seg_ptr->mutex, 0, sizeof(std::shared_mutex));
            memset((void *) &seg_ptr->rwlock, 0, sizeof(PMEMrwlock));
            memset((void *) &seg_ptr->_[0], 255, sizeof(_Pair<KV>) * kNumSlot);
            return 0;
        };
        // pmemobj_alloc(pool_addr, )
        Allocator::Allocate((void **) seg, kCacheLineSize, sizeof(Segment),
                            callback, reinterpret_cast<void *>(&depth));
#else
        Allocator::ZAllocate((void **) seg, kCacheLineSize, sizeof(Segment));
        new (*seg) Segment(depth);
#endif
    }

    static void New(PMEMoid *seg, size_t depth) {
#ifdef PMEM
        auto callback = [](PMEMobjpool *pool, void *ptr, void *arg) {
            auto value_ptr = reinterpret_cast<size_t *>(arg);
            auto seg_ptr = reinterpret_cast<Segment *>(ptr);
            seg_ptr->local_depth = *value_ptr;
            seg_ptr->sema = 0;
            seg_ptr->count = 0;
            seg_ptr->seg_lock = 0;
            memset((void *) &seg_ptr->mutex, 0, sizeof(std::shared_mutex));
            memset((void *) &seg_ptr->rwlock, 0, sizeof(PMEMrwlock));
            memset((void *) &seg_ptr->_[0], 255, sizeof(_Pair<KV>) * kNumSlot);
            Allocator::Persist<KV>(seg_ptr, sizeof(Segment<KV>));
            return 0;
        };
        Allocator::Allocate(seg, kCacheLineSize, sizeof(Segment), callback,
                            reinterpret_cast<void *>(&depth));
#endif
    }

    ~Segment(void) {}

    int Insert(PMEMobjpool *, std::string_view, std::string_view, size_t,
               size_t, c_ptr<typename KV::K_TYPE>, c_ptr<typename KV::V_TYPE>);
    int Insert4split(c_ptr<typename KV::K_TYPE>, c_ptr<typename KV::V_TYPE>,
                     size_t);
    bool Put(std::string_view, std::string_view, size_t);
    PMEMoid *Split(PMEMobjpool *, size_t, log_entry *);

    void get_lock(PMEMobjpool *pop) {
#ifdef PERSISTENT_LOCK
        pmemobj_rwlock_wrlock(pop, &rwlock);
#else
        mutex.lock();
#endif
    }

    void release_lock(PMEMobjpool *pop) {
#ifdef PERSISTENT_LOCK
        pmemobj_rwlock_unlock(pop, &rwlock);
#if defined(COUNTING_WRITE)
        /* The lock will be flushed back to PM, so we add a count here */
        pmhb_ns::sample_guard<CCEH_COW<KV>, pmhb_ns::WRITE_COUNT>{
                sizeof(rwlock)};
#endif
#else
        mutex.unlock();
#endif
    }

    void get_rd_lock(PMEMobjpool *pop) {
#ifdef PERSISTENT_LOCK
        pmemobj_rwlock_rdlock(pop, &rwlock);
#else
        mutex.lock_shared();
#endif
    }

    void release_rd_lock(PMEMobjpool *pop) {
#ifdef PERSISTENT_LOCK
        pmemobj_rwlock_unlock(pop, &rwlock);
#else
        mutex.unlock_shared();
#endif
    }

    bool try_get_lock(PMEMobjpool *pop) {
#ifdef PERSISTENT_LOCK
        if (pmemobj_rwlock_trywrlock(pop, &rwlock) == 0) { return true; }
        return false;
#else
        return mutex.try_lock();
#endif
    }

    bool try_get_rd_lock(PMEMobjpool *pop) {
#ifdef PERSISTENT_LOCK
        if (pmemobj_rwlock_tryrdlock(pop, &rwlock) == 0) { return true; }
        return false;
#else
        return mutex.try_lock_shared();
#endif
    }

    _Pair<KV> _[kNumSlot];
    size_t local_depth;
    int64_t sema = 0;
    size_t pattern = 0;
    int count = 0;
    std::shared_mutex mutex;
    uint64_t seg_lock;
    PMEMrwlock rwlock;
};

template<class KV>
struct Seg_array {
    typedef Segment<KV> *seg_p;
    size_t global_depth;
    seg_p _[0];

    static void New(PMEMoid *sa, size_t capacity) {
#ifdef PMEM
        auto callback = [](PMEMobjpool *pool, void *ptr, void *arg) {
            auto value_ptr = reinterpret_cast<size_t *>(arg);
            auto sa_ptr = reinterpret_cast<Seg_array *>(ptr);
            sa_ptr->global_depth = static_cast<size_t>(log2(*value_ptr));
            memset(sa_ptr->_, 0, (*value_ptr) * sizeof(uint64_t));
            return 0;
        };
        Allocator::Allocate(sa, kCacheLineSize,
                            sizeof(Seg_array) + sizeof(uint64_t) * capacity,
                            callback, reinterpret_cast<void *>(&capacity));
#else
        Allocator::ZAllocate((void **) sa, kCacheLineSize, sizeof(Seg_array));
        new (*sa) Seg_array(capacity);
#endif
    }
};

template<class KV>
struct Directory {
    static const size_t kDefaultDirectorySize = 1024;
    Seg_array<KV> *sa;
    PMEMoid new_sa;
    size_t capacity;
    bool lock;
    int sema = 0;

    Directory(Seg_array<KV> *_sa) {
        capacity = kDefaultDirectorySize;
        sa = _sa;
        new_sa = OID_NULL;
        lock = false;
        sema = 0;
    }

    Directory(size_t size, Seg_array<KV> *_sa) {
        capacity = size;
        sa = _sa;
        new_sa = OID_NULL;
        lock = false;
        sema = 0;
    }

    static void New(Directory **dir, size_t capacity) {
#ifdef PMEM
        auto callback = [](PMEMobjpool *pool, void *ptr, void *arg) {
            auto value_ptr =
                    reinterpret_cast<std::pair<size_t, Seg_array<KV> *> *>(arg);
            auto dir_ptr = reinterpret_cast<Directory *>(ptr);
            dir_ptr->capacity = value_ptr->first;
            dir_ptr->sa = value_ptr->second;
            dir_ptr->new_sa = OID_NULL;
            dir_ptr->lock = false;
            dir_ptr->sema = 0;
            dir_ptr = nullptr;
            return 0;
        };

        auto call_args = std::make_pair(capacity, nullptr);
        Allocator::Allocate((void **) dir, kCacheLineSize, sizeof(Directory),
                            callback, reinterpret_cast<void *>(&call_args));
#else
        Allocator::ZAllocate((void **) dir, kCacheLineSize, sizeof(Directory));
        new (*dir) Directory(capacity, temp_sa);
#endif
    }

    ~Directory(void) {}

    void get_item_num() {
        size_t count = 0;
        size_t seg_num = 0;
        Seg_array<KV> *seg = sa;
        Segment<KV> **dir_entry = seg->_;
        Segment<KV> *ss;
        auto global_depth = seg->global_depth;
        size_t depth_diff;
        for (int i = 0; i < capacity;) {
            ss = dir_entry[i];
            depth_diff = global_depth - ss->local_depth;

            for (unsigned i = 0; i < Segment<KV>::kNumSlot; ++i) {
                if ((ss->_[i].key != INVALID) &&
                    ((h((void *) ss->_[i].key->data(),
                        strlen(ss->_[i].key->data())) >>
                      (64 - ss->local_depth)) == ss->pattern)) {
                    ++count;
                }
            }

            seg_num++;
            i += pow(2, depth_diff);
        }
        // std::cout << "#items: " << count << std::endl;
        std::cout << "load_factor: " << (double) count / (seg_num * 256 * 4)
                  << std::endl;
    }

    bool Acquire(void) {
        bool unlocked = false;
        return CCEH_COW_CAS(&lock, &unlocked, true);
    }

    bool Release(void) {
        bool locked = true;
        return CCEH_COW_CAS(&lock, &locked, false);
    }

    void SanityCheck(void *);
};

template<class KV>
class CCEH_COW {
public:
    CCEH_COW(void);
    CCEH_COW(int, PMEMobjpool *_pool);
    ~CCEH_COW(void);

    // static CCEH_COW *open(std::filesystem::path pool_path, size_t pool_size,
    //                   size_t init_depth);
    int
    Insert(std::string_view key, std::string_view value,
           c_ptr<typename KV::K_TYPE> k_ptr = c_ptr<typename KV::K_TYPE>(0ul),
           c_ptr<typename KV::V_TYPE> v_ptr = c_ptr<typename KV::V_TYPE>(0ul));
    int Insert(std::string_view key, std::string_view value, bool);
    bool
    Update(std::string_view key, std::string_view value,
           c_ptr<typename KV::K_TYPE> k_ptr = c_ptr<typename KV::K_TYPE>(0ul),
           c_ptr<typename KV::V_TYPE> v_ptr = c_ptr<typename KV::V_TYPE>(0ul));
    bool Delete(std::string_view);
    bool Delete(std::string_view, bool);
    Value_t Get(std::string_view);
    Value_t Get(std::string_view, bool is_in_epoch);
    Value_t FindAnyway(std::string_view);
    double Utilization(void);
    size_t Capacity(void);
    void Recovery(PMEMobjpool *_pop);
    void Directory_Doubling(int x, Segment<KV> *s0, PMEMoid *s1);
    void Directory_Update(int x, Segment<KV> *s0, PMEMoid *s1);
    void Lock_Directory();
    void Unlock_Directory();
    void TX_Swap(void **entry, PMEMoid *new_seg);
    void getNumber() { dir->get_item_num(); }
    size_t get_memory_usage();

    Directory<KV> *dir;
    log_entry log[LOG_NUM];
    int seg_num;
    int restart;
#ifdef PMEM
    PMEMobjpool *pool_addr;
#endif
};

// template<class KV>
// static CCEH_COW<KV> *open(std::filesystem::path pool_path, size_t pool_size,
//                       size_t init_depth) {
//     CCEH_COW<KV> *ret = nullptr;
//     if (std::filesystem::exists(pool_path)) {
//         auto pop = pmem::obj::pool<CCEH_COW<KV>>::open(pool_path, "CCEH_COW");
//         ret = pop.root().get();
//     } else {
//         auto pop =
//                 pmem::obj::pool<CCEH_COW<KV>>::create(pool_path, "CCEH_COW", pool_size);
//         pre_fault(pop.handle(), pool_size);
//         new (ret) CCEH_COW<KV>(init_depth, pop.handle());
//     }
// }

template<class KV>
int Segment<KV>::Insert(PMEMobjpool *pool_addr, std::string_view key,
                        std::string_view value, size_t loc, size_t key_hash,
                        c_ptr<typename KV::K_TYPE> k_ptr,
                        c_ptr<typename KV::V_TYPE> v_ptr) {
    if (sema == -1) { return 2; };
    if (!try_get_lock(pool_addr)) { return 2; }
    // get_lock(pool_addr);
    if ((key_hash >> (8 * sizeof(key_hash) - local_depth)) != pattern ||
        sema == -1) {
        release_lock(pool_addr);
        return 2;
    }
    int ret = 1;
    size_t LOCK = INVALID;

    /*uniqueness check*/
    auto slot = loc;
    for (unsigned i = 0; i < kNumCacheLine * kNumPairPerCacheLine; ++i) {
        slot = (loc + i) % kNumSlot;
        // if (_[slot].key != INVALID) {
        //     fmt::print("    the offset is {}\n", _[slot].key.offset);
        //     fmt::print("    data() is {}\n", _[slot].key->data());
        //     std::cout << std::flush;
        // }
        if (_[slot].key != INVALID && (key == _[slot].key->data())) {
            release_lock(pool_addr);
            return -3;
        }
    }
    for (unsigned i = 0; i < kNumPairPerCacheLine * kNumCacheLine; ++i) {
        slot = (loc + i) % kNumSlot;
        if ((_[slot].key != INVALID) &&
            ((h(_[slot].key->data(), strlen(_[slot].key->data())) >>
              (8 * sizeof(key_hash) - local_depth)) != pattern)) {
            _[slot].key = INVALID;
        }
        if (CCEH_COW_CAS(&_[slot].key.offset, &LOCK, SENTINEL)) {
            if (k_ptr == nullptr && v_ptr == nullptr) {
                PMEMoid p_kv;
                c_ptr<KV> pkv;

#ifdef WRITE_KV
                auto construct = [](PMEMobjpool *pop, void *ptr, void *arg) {
                    new (ptr) KV(arg);
                    return 0;
                };
                std::tuple construc_args = {16u, key, 16u, value};
                pmemobj_alloc(pool_addr, &p_kv, 40, 40, construct,
                              (void *) &construc_args);
#endif

                pkv.offset = p_kv.off;

                _[slot].key =
                        c_ptr<typename KV::K_TYPE>(pmemobj_oid(pkv->key()).off);
                _[slot].value = c_ptr<typename KV::V_TYPE>(
                        pmemobj_oid(pkv->value()).off);
            } else {
                _[slot].key = k_ptr;
                _[slot].value = v_ptr;
            }
            Allocator::Persist<KV>(&_[slot], sizeof(_Pair<KV>));
            ret = 0;
            break;
        } else {
            LOCK = INVALID;
        }
    }
    release_lock(pool_addr);
    return ret;
}

template<class KV>
int Segment<KV>::Insert4split(c_ptr<typename KV::K_TYPE> key,
                              c_ptr<typename KV::V_TYPE> value, size_t loc) {
    for (unsigned i = 0; i < kNumPairPerCacheLine * kNumCacheLine; ++i) {
        auto slot = (loc + i) % kNumSlot;
        if (_[slot].key == INVALID) {
            _[slot].key = key;
            _[slot].value = value;
            return 0;
        }
    }
    return -1;
}

template<class KV>
PMEMoid *Segment<KV>::Split(PMEMobjpool *pool_addr, size_t key_hash,
                            log_entry *log) {
#ifdef PMHB_LATENCY
    auto g = pmhb_ns::sample_guard<CCEH_COW<KV>, pmhb_ns::REHASH>{};
#endif
    using namespace std;
    if (!try_get_lock(pool_addr)) { return nullptr; }
    sema = -1;

    size_t new_pattern = (pattern << 1) + 1;
    size_t old_pattern = pattern << 1;

    uint64_t log_pos = key_hash % LOG_NUM;
    // log[log_pos].Lock_log();

    PMEMoid *split_oid = new PMEMoid[2];
    // New two segments here (CoW)
    // Segment::New(&log[log_pos].temp, local_depth + 1);
    Segment::New(&split_oid[0], local_depth + 1);
    Segment::New(&split_oid[1], local_depth + 1);
    // Segment<KV> *split =
    //         reinterpret_cast<Segment<KV> *>(pmemobj_direct(log[log_pos].temp));
    Segment<KV> *split[2] = {(Segment<KV> *) pmemobj_direct(split_oid[0]),
                             (Segment<KV> *) pmemobj_direct(split_oid[1])};

    split[0]->pattern = old_pattern;
    split[1]->pattern = new_pattern;

    size_t involved_kv = 0;
    for (unsigned i = 0; i < kNumSlot; ++i) {
        uint64_t key_hash;
        if (_[i].key != INVALID) {
            key_hash = h(_[i].key->data(), strlen(_[i].key->data()));
            if ((key_hash >> (8 * 8 - local_depth - 1) == new_pattern)) {
                split[1]->Insert4split(_[i].key, _[i].value,
                                       (key_hash & kMask) *
                                               kNumPairPerCacheLine);
            } else {
                split[0]->Insert4split(_[i].key, _[i].value,
                                       (key_hash & kMask) *
                                               kNumPairPerCacheLine);
            }
            involved_kv++;
        }
    }
#ifdef PMHB_LATENCY
    pmhb_ns::sample_guard<CCEH_COW<KV>, pmhb_ns::RESIZE_ITEM_NUMBER>{
            involved_kv};
#endif

#ifdef PMEM
    Allocator::Persist<KV>(split[0], sizeof(Segment<KV>));
    Allocator::Persist<KV>(split[1], sizeof(Segment<KV>));
#endif
    sema = 0;

    return split_oid;
}

template<class KV>
CCEH_COW<KV>::CCEH_COW(int initCap, PMEMobjpool *_pool) {
    Directory<KV>::New(&dir, initCap);
    Seg_array<KV>::New(&dir->new_sa, initCap);
    dir->sa = reinterpret_cast<Seg_array<KV> *>(pmemobj_direct(dir->new_sa));
    dir->new_sa = OID_NULL;
    auto dir_entry = dir->sa->_;
    for (int i = 0; i < dir->capacity; ++i) {
        Segment<KV>::New(&dir_entry[i], dir->sa->global_depth);
        dir_entry[i]->pattern = i;
    }
    /*clear the log area*/
    for (int i = 0; i < LOG_NUM; ++i) {
        log[i].lock = 0;
        log[i].temp = OID_NULL;
    }

    seg_num = 0;
    restart = 0;
    pool_addr = _pool;

    /* Initialize the compound pointer */
    c_ptr<typename KV::K_TYPE>::pool_uuid_lo = pmemobj_oid(_pool).pool_uuid_lo;
    c_ptr<typename KV::V_TYPE>::pool_uuid_lo = pmemobj_oid(_pool).pool_uuid_lo;
    c_ptr<KV>::pool_uuid_lo = pmemobj_oid(_pool).pool_uuid_lo;
}

template<class KV>
CCEH_COW<KV>::CCEH_COW(void) {
    std::cout << "Reintialize Up for CCEH_COW" << std::endl;
}

template<class KV>
CCEH_COW<KV>::~CCEH_COW(void) {}

template<class KV>
void CCEH_COW<KV>::Recovery(PMEMobjpool *_pop) {
    c_ptr<typename KV::K_TYPE>::pool_uuid_lo = pmemobj_oid(_pop).pool_uuid_lo;
    c_ptr<typename KV::V_TYPE>::pool_uuid_lo = pmemobj_oid(_pop).pool_uuid_lo;
    c_ptr<KV>::pool_uuid_lo = pmemobj_oid(_pop).pool_uuid_lo;
    for (int i = 0; i < LOG_NUM; ++i) {
        if (!OID_IS_NULL(log[i].temp)) { pmemobj_free(&log[i].temp); }
    }

    if (dir != nullptr) {
        dir->lock = 0;
        if (!OID_IS_NULL(dir->new_sa)) { pmemobj_free(&dir->new_sa); }

        if (dir->sa == nullptr) return;
        auto dir_entry = dir->sa->_;
        size_t global_depth = dir->sa->global_depth;
        size_t depth_cur, buddy, stride, i = 0;
        /*Recover the Directory*/
        size_t seg_count = 0;
        while (i < dir->capacity) {
            auto target = dir_entry[i];
            depth_cur = target->local_depth;
            target->sema = 0;
            stride = pow(2, global_depth - depth_cur);
            buddy = i + stride;
            for (int j = buddy - 1; j > i; j--) {
                target = dir_entry[j];
                if (dir_entry[j] != dir_entry[i]) {
                    dir_entry[j] = dir_entry[i];
                    target->pattern = i >> (global_depth - depth_cur);
                }
            }
            seg_count++;
            i = i + stride;
        }
    }
}

template<class KV>
void CCEH_COW<KV>::TX_Swap(void **entry, PMEMoid *new_seg) {
    TX_BEGIN(pool_addr) {
        pmemobj_tx_add_range_direct(entry, sizeof(void *));
        pmemobj_tx_add_range_direct(new_seg, sizeof(PMEMoid));
        *entry = pmemobj_direct(*new_seg);
        *new_seg = OID_NULL;
    }
    TX_ONABORT { std::cout << "Error in TXN Swap!" << std::endl; }
    TX_END
}

template<class KV>
void CCEH_COW<KV>::Directory_Doubling(int x, Segment<KV> *s0,
                                      PMEMoid *split_oid) {
#ifdef PMHB_LATENCY
    auto g = pmhb_ns::sample_guard<CCEH_COW<KV>, pmhb_ns::DOUBLE>{};
#endif
    Seg_array<KV> *sa = dir->sa;
    Segment<KV> **d = sa->_;
    auto global_depth = sa->global_depth;

    /* new segment array*/
    Seg_array<KV>::New(&dir->new_sa, 2 * dir->capacity);
    auto new_seg_array =
            reinterpret_cast<Seg_array<KV> *>(pmemobj_direct(dir->new_sa));
    auto dd = new_seg_array->_;

    for (unsigned i = 0; i < dir->capacity; ++i) {
        dd[2 * i] = d[i];
        dd[2 * i + 1] = d[i];
    }

    dd[2 * x + 1] = (Segment<KV> *) pmemobj_direct(split_oid[1]);
    dd[2 * x] = (Segment<KV> *) pmemobj_direct(split_oid[0]);

    // TX_Swap((void **) &dd[2 * x + 1], &split_oid[1]);
    // TX_Swap((void **) &dd[2 * x], &split_oid[0]);

#ifdef PMEM
    Allocator::Persist<KV>(new_seg_array,
                           sizeof(Seg_array<KV>) +
                                   sizeof(Segment<KV> *) * 2 * dir->capacity);
#endif
    TX_BEGIN(pool_addr) {
        pmemobj_tx_add_range_direct(&dir->sa, sizeof(dir->sa));
        pmemobj_tx_add_range_direct(&dir->new_sa, sizeof(dir->new_sa));
        pmemobj_tx_add_range_direct(&dir->capacity, sizeof(dir->capacity));
        // pmem_free(sa);
        dir->sa =
                reinterpret_cast<Seg_array<KV> *>(pmemobj_direct(dir->new_sa));
        dir->new_sa = OID_NULL;
        dir->capacity *= 2;
    }
    TX_ONABORT {
        std::cout << "TXN fails during doubling directory" << std::endl;
    }
    TX_END
}

template<class KV>
void CCEH_COW<KV>::Lock_Directory() {
    while (!dir->Acquire()) { asm("nop"); }
}

template<class KV>
void CCEH_COW<KV>::Unlock_Directory() {
    while (!dir->Release()) { asm("nop"); }
}

template<class KV>
void CCEH_COW<KV>::Directory_Update(int x, Segment<KV> *s0,
                                    PMEMoid *split_oid) {
    Segment<KV> **dir_entry = dir->sa->_;
    Segment<KV> *split[2] = {(Segment<KV> *) pmemobj_direct(split_oid[0]),
                             (Segment<KV> *) pmemobj_direct(split_oid[1])};
    auto global_depth = dir->sa->global_depth;
    unsigned depth_diff = global_depth - s0->local_depth;
    if (depth_diff == 1) {
        x |= 1;
        dir_entry[x] = split[1];
        Allocator::Persist<KV>(&dir_entry[x], sizeof(Segment<KV> *));
        dir_entry[x - 1] = split[0];
        Allocator::Persist<KV>(&dir_entry[x - 1], sizeof(Segment<KV> *));
    } else {
        int chunk_size = pow(2, global_depth - (s0->local_depth));
        x = x - (x % chunk_size);
        int base = chunk_size / 2;
        for (int i = base - 1; i >= 0; --i) {
            dir_entry[x + base + i] = split[1];
            Allocator::Persist<KV>(&dir_entry[x + base + i], sizeof(uint64_t));
        }
        for (int i = base - 1; i >= 0; --i) {
            dir_entry[x + i] = split[0];
            Allocator::Persist<KV>(&dir_entry[x + i], sizeof(uint64_t));
        }
    }
}

template<class KV>
int CCEH_COW<KV>::Insert(std::string_view key, std::string_view value,
                         bool is_in_epoch) {
    if (!is_in_epoch) { return Insert(key, value); }
    return Insert(key, value);
}

template<class KV>
int CCEH_COW<KV>::Insert(std::string_view key, std::string_view value,
                         c_ptr<typename KV::K_TYPE> k_ptr,
                         c_ptr<typename KV::V_TYPE> v_ptr) {
#ifdef PMHB_LATENCY
    auto g = pmhb_ns::sample_guard<CCEH_COW<KV>, pmhb_ns::INSERT>{};
#endif

#ifdef INSERT_DEBUG
    time_guard tg("Insert");
#endif


STARTOVER:
    uint64_t key_hash;
    key_hash = h(key.data(), strlen(key.data()));
    auto y = (key_hash & kMask) * kNumPairPerCacheLine;

RETRY:
    // CC_LOG("{}", key.data());
    auto old_sa = dir->sa;
    auto x = (key_hash >> (8 * sizeof(key_hash) - old_sa->global_depth));
    auto dir_entry = old_sa->_;
    Segment<KV> *target = dir_entry[x];
    if (old_sa != dir->sa) { goto RETRY; }

    auto ret = target->Insert(pool_addr, key, value, y, key_hash, k_ptr, v_ptr);

    if (ret == -3) return -1;

    if (ret == 1) {
        PMEMoid *split_oid;
        {
#ifdef INSERT_DEBUG
            time_guard tg1("split", tg);
#endif
            split_oid = target->Split(pool_addr, key_hash, log);
        }
        if (split_oid == nullptr) { goto RETRY; }
        // auto ss = reinterpret_cast<Segment<KV> *>(pmemobj_direct(*split_oid));
        Segment<KV> *split[2] = {(Segment<KV> *) pmemobj_direct(split_oid[0]),
                                 (Segment<KV> *) pmemobj_direct(split_oid[1])};
        // delete[] split_oid;
        // split[1]->pattern = ((key_hash >> (8 * sizeof(key_hash) -
        //                                    split[1]->local_depth + 1))
        //                      << 1) +
        //                     1;
        // split[0]->pattern = split[1]->pattern - 1;
        // Allocator::Persist<KV>(&split[1]->pattern, sizeof(split[1]->pattern));
        // Allocator::Persist<KV>(&split[0]->pattern, sizeof(split[0]->pattern));

        // Directory management
        Lock_Directory();
        {// CRITICAL SECTION - directory update
            auto sa = dir->sa;
            dir_entry = sa->_;
            Segment<KV> *target_ = target;
            x = (key_hash >> (8 * sizeof(key_hash) - sa->global_depth));
            target = dir_entry[x];
            if (target != target_) {
                fmt::print("Not equal\n");
                fmt::print("Pattern: Old = {}, New = {}\n", target_->pattern,
                           target->pattern);
                fmt::print("Local: Old = {}, New = {}\n", target_->local_depth,
                           target->local_depth);
                exit(1);
            }
            if (target->local_depth < sa->global_depth) {
                Directory_Update(x, target, split_oid);
            } else {// directory doubling
#ifdef INSERT_DEBUG
                time_guard tg1("double", tg);
#endif
                Directory_Doubling(x, target, split_oid);
            }
            delete[] split_oid;

            // target can be deleted
            //             target->pattern =
            //                     (key_hash >> (8 * sizeof(key_hash) - target->local_depth))
            //                     << 1;
            //             Allocator::Persist<KV>(&target->pattern, sizeof(target->pattern));
            //             target->local_depth += 1;
            //             Allocator::Persist<KV>(&target->local_depth,
            //                                    sizeof(target->local_depth));
            // #ifdef INPLACE
            // #endif
            // target->sema = 0;
            // target->release_lock(pool_addr);
        }// End of critical section
        Unlock_Directory();
        // uint64_t log_pos = key_hash % LOG_NUM;
        // log[log_pos].Unlock_log();

        goto RETRY;
    } else if (ret == 2) {
        goto STARTOVER;
    }

    return 0;
}


template<class KV>
bool CCEH_COW<KV>::Update(std::string_view key, std::string_view value,
                          c_ptr<typename KV::K_TYPE> k_ptr,
                          c_ptr<typename KV::V_TYPE> v_ptr) {
#ifdef PMHB_LATENCY
    auto g = pmhb_ns::sample_guard<CCEH_COW<KV>, pmhb_ns::UPDATE>{};
#endif
    uint64_t key_hash;
    key_hash = h(key.data(), strlen(key.data()));
    auto y = (key_hash & kMask) * kNumPairPerCacheLine;

RETRY:
    auto old_sa = dir->sa;
    auto x = (key_hash >> (8 * sizeof(key_hash) - old_sa->global_depth));
    auto dir_entry = old_sa->_;
    Segment<KV> *dir_ = dir_entry[x];

    auto sema = dir_->sema;
    if (sema == -1) { goto RETRY; }
    dir_->get_lock(pool_addr);

    if ((key_hash >> (8 * sizeof(key_hash) - dir_->local_depth)) !=
                dir_->pattern ||
        dir_->sema == -1) {
        dir_->release_lock(pool_addr);
        goto RETRY;
    }

    for (unsigned i = 0; i < kNumPairPerCacheLine * kNumCacheLine; ++i) {
        auto slot = (y + i) % Segment<KV>::kNumSlot;
        if ((dir_->_[slot].key != INVALID) &&
            (key == dir_->_[slot].key->data())) {
            dir_->_[slot].value = v_ptr;
            Allocator::Persist<KV>(&dir_->_[slot], sizeof(_Pair<KV>));
            dir_->release_lock(pool_addr);
            return true;
        }
    }
    dir_->release_lock(pool_addr);
    return false;
}


template<class KV>
bool CCEH_COW<KV>::Delete(std::string_view key, bool is_in_epoch) {
    if (!is_in_epoch) { return Delete(key); }
    return Delete(key);
}

template<class KV>
bool CCEH_COW<KV>::Delete(std::string_view key) {
#ifdef PMHB_LATENCY
    auto g = pmhb_ns::sample_guard<CCEH_COW<KV>, pmhb_ns::DELETE>{};
#endif
    uint64_t key_hash;
    key_hash = h(key.data(), strlen(key.data()));
    auto y = (key_hash & kMask) * kNumPairPerCacheLine;

RETRY:
    auto old_sa = dir->sa;
    auto x = (key_hash >> (8 * sizeof(key_hash) - old_sa->global_depth));
    auto dir_entry = old_sa->_;
    Segment<KV> *dir_ = dir_entry[x];

    auto sema = dir_->sema;
    if (sema == -1) { goto RETRY; }
    dir_->get_lock(pool_addr);

    if ((key_hash >> (8 * sizeof(key_hash) - dir_->local_depth)) !=
                dir_->pattern ||
        dir_->sema == -1) {
        dir_->release_lock(pool_addr);
        goto RETRY;
    }

    for (unsigned i = 0; i < kNumPairPerCacheLine * kNumCacheLine; ++i) {
        auto slot = (y + i) % Segment<KV>::kNumSlot;
        if ((dir_->_[slot].key != INVALID) &&
            (key == dir_->_[slot].key->data())) {
            dir_->_[slot].key = INVALID;
            Allocator::Persist<KV>(&dir_->_[slot], sizeof(_Pair<KV>));
            dir_->release_lock(pool_addr);
            return true;
        }
    }
    dir_->release_lock(pool_addr);
    return false;
}

template<class KV>
Value_t CCEH_COW<KV>::Get(std::string_view key, bool is_in_epoch) {
    if (is_in_epoch) { return Get(key); }
    return Get(key);
}

template<class KV>
Value_t CCEH_COW<KV>::Get(std::string_view key) {
#ifdef PMHB_LATENCY
    auto g = pmhb_ns::sample_guard<CCEH_COW<KV>, pmhb_ns::SEARCH>{};
#endif
    uint64_t key_hash;
    key_hash = h(key.data(), strlen(key.data()));
    auto y = (key_hash & kMask) * kNumPairPerCacheLine;

RETRY:
    auto old_sa = dir->sa;
    auto x = (key_hash >> (8 * sizeof(key_hash) - old_sa->global_depth));
    auto dir_entry = old_sa->_;
    Segment<KV> *dir_ = dir_entry[x];

    // if (!dir_->try_get_rd_lock(pool_addr)) { goto RETRY; }

    if ((key_hash >> (8 * sizeof(key_hash) - dir_->local_depth)) !=
        dir_->pattern) {
        // dir_->release_rd_lock(pool_addr);
        // if ((key_hash >> (8 * sizeof(key_hash) - dir_->local_depth)) !=
        //     dir_->pattern)
        //     fmt::print("retry because pattern\n");
        // else
        //     fmt::print("sema = -1\n");
        goto RETRY;
    }

    for (unsigned i = 0; i < kNumPairPerCacheLine * kNumCacheLine; ++i) {
        auto slot = (y + i) % Segment<KV>::kNumSlot;
        auto k = dir_->_[slot].key;
        if ((k != INVALID) && (k != SENTINEL) && (key == k->data())) {
            auto value = dir_->_[slot].value;
            // dir_->release_rd_lock(pool_addr);
            return value->data();
        }
    }

    // dir_->release_rd_lock(pool_addr);
    return NONE;
}

template<class KV>
size_t CCEH_COW<KV>::get_memory_usage() {
    /* traverse all structure */

    size_t sum = 0;

    const auto &d = *dir->sa;
    size_t capacity = 1ul << d.global_depth;
    Segment<KV> *last_pointer = nullptr;

    for (size_t i = 0; i < capacity; i++) {
        if (last_pointer - d._[i] == 0) {
            continue;
        } else {
            sum += sizeof(Segment<KV>);
            last_pointer = d._[i];
        }
    }
    /* Directory */
    sum += sizeof(Segment<KV> *) * capacity + sizeof(Directory<KV>);
    return sum;
}

}// namespace cceh_cow_ns