#ifndef PMEMOBJ_CLHT_HPP
#define PMEMOBJ_CLHT_HPP

#include "compound_pool_ptr.hpp"
#include "make_persistent_object.hpp"
#include <libpmemobj++/detail/common.hpp>
#include <libpmemobj++/detail/persistent_pool_ptr.hpp>
#include <libpmemobj++/detail/specialization.hpp>
#include <libpmemobj++/detail/template_helpers.hpp>
// #include <libpmemobj++/experimental/concurrent_hash_map.hpp>
// #include <libpmemobj++/experimental/hash.hpp>
#include <libpmemobj++/experimental/v.hpp>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/make_persistent_atomic.hpp>
#include <libpmemobj++/mutex.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/transaction.hpp>

#include <atomic>
#include <cassert>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <fmt/core.h>
#include <fmt/ranges.h>
#include <fstream>
#include <functional>
#include <initializer_list>
#include <iostream>
#include <iterator>// for std::distance
#include <memory>
#include <mutex>
#include <sstream>
#include <thread>
#include <time.h>
#include <type_traits>
#include <vector>

#if defined PMHB_LATENCY || defined COUNTING_WRITE
#include "../../include/sample_guard.hpp"
#endif


// bool __sync_val_compare_and_swap (type *ptr, type oldval, type newval)
/*
These builtins perform an atomic compare and swap. That is, if the current
value of *ptr is oldval, then write newval into *ptr. The “bool” version
returns true if the comparison is successful and newval was written.

NOTE: oldval will NOT be updated if the atomic operation fails.
*/
#define CLHT_CAS(ptr, oldval, newval)                                          \
    (__sync_val_compare_and_swap(ptr, oldval, newval))

//test-and-set uint8_t
// set *addr to 0xFF and return the old value in addr
static inline uint8_t tas_uint8(volatile uint8_t *addr) {
    uint8_t oldval;
    __asm__ __volatile__("xchgb %0,%1"
                         : "=q"(oldval), "=m"(*addr)
                         : "0"((unsigned char) 0xff), "m"(*addr)
                         : "memory");
    return (uint8_t) oldval;
}

#define likely(x) __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)
#define ALIGNED(N) __attribute__((aligned(N)))

#define CLHT_READ_ONLY_FAIL 1
#define CLHT_HELP_RESIZE 1
#define CLHT_PERC_EXPANSIONS 1
#define CLHT_MAX_EXPANSIONS 24
#define CLHT_PERC_FULL_DOUBLE 50 /* % */
#define CLHT_RATIO_DOUBLE 2
#define CLHT_OCCUP_AFTER_RES 40
#define CLHT_PERC_FULL_HALVE 5 /* % */
#define CLHT_RATIO_HALVE 8
#define CLHT_MIN_CLHT_SIZE 8
#define CLHT_DO_CHECK_STATUS 0
#define CLHT_DO_GC 0
#define CLHT_STATUS_INVOK 500000
#define CLHT_STATUS_INVOK_IN 500000

#define CACHE_LINE_SIZE 64
#define ENTRIES_PER_BUCKET 3

#define LOCK_FREE 0
#define LOCK_UPDATE 1
#define LOCK_RESIZE 2


// #define SEARCH_DEBUG
// #define UPDATE_DEBUG


namespace pclht_ns {

using namespace pmem::obj;

class atomic_backoff {
    /**
	 * Time delay, in units of "pause" instructions.
	 * Should be equal to approximately the number of "pause" instructions
	 * that take the same time as an context switch. Must be a power of two.
	 */
    static const int32_t LOOPS_BEFORE_YIELD = 16;
    int32_t count;

    static inline void __pause(int32_t delay) {
        for (; delay > 0; --delay) {
#if _MSC_VER
            YieldProcessor();
#elif __GNUC__ && (__i386__ || __x86_64__)
            // Only i386 and x86-64 have pause instruction
            __builtin_ia32_pause();
#endif
        }
    }

public:
    /**
	 * Deny copy constructor
	 */
    atomic_backoff(const atomic_backoff &) = delete;
    /**
	 * Deny assignment
	 */
    atomic_backoff &operator=(const atomic_backoff &) = delete;

    /** Default constructor */
    /* In many cases, an object of this type is initialized eagerly on hot
	 * path, as in for(atomic_backoff b; ; b.pause()) {...} For this reason,
	 * the construction cost must be very small! */
    atomic_backoff() : count(1) {}

    /**
	 * This constructor pauses immediately; do not use on hot paths!
	 */
    atomic_backoff(bool) : count(1) { pause(); }

    /**
	 * Pause for a while.
	 */
    void pause() {
        if (count <= LOOPS_BEFORE_YIELD) {
            __pause(count);
            /* Pause twice as long the next time. */
            count *= 2;
        } else {
            /* Pause is so long that we might as well yield CPU to
			 * scheduler. */
            std::this_thread::yield();
        }
    }

    /**
	 * Pause for a few times and return false if saturated.
	 */
    bool bounded_pause() {
        __pause(count);
        if (count < LOOPS_BEFORE_YIELD) {
            /* Pause twice as long the next time. */
            count *= 2;
            return true;
        } else {
            return false;
        }
    }

    void reset() { count = 1; }
}; /* class atomic_backoff */


template<typename Key, typename T, typename Hash = std::hash<Key>,
         typename KeyEqual = std::equal_to<Key>>
class clht {
public:
    using key_type = Key;
    using mapped_type = T;
    using value_type = std::pair<const Key, T>;
    using size_type = size_t;
    using difference_type = ptrdiff_t;
    using pointer = value_type *;
    using const_pointer = const value_type *;
    using reference = value_type &;
    using const_reference = const value_type &;

    using hasher = Hash;
    using key_equal = typename std::equal_to<Key>;

    // using atomic_backoff = atomic_backoff;

    struct bucket_s;
    struct clht_hashtable_s;
    struct ht_ts;

    using hv_type = size_t;
    using clht_lock_t = volatile uint8_t;
    using kv_ptr_t = compound_pool_ptr<value_type>;
    using bucket_ptr_t = compound_pool_ptr<bucket_s>;
    using clht_hashtable_ptr_t = compound_pool_ptr<clht_hashtable_s>;
    using ht_ts_ptr_t = compound_pool_ptr<ht_ts>;
    using clht_table_t = persistent_ptr<bucket_s[]>;

    struct ret {
        bool found;
        difference_type bucket_idx;
        uint8_t step;
        uint8_t slot_idx;
        bool expanded;
        uint64_t capacity;

        ret(difference_type _bucket_idx, uint8_t _step, uint8_t _slot_idx,
            size_type _expanded = false, uint64_t _cap = 0)
            : found(true), bucket_idx(_bucket_idx), step(_step),
              slot_idx(_slot_idx), expanded(_expanded), capacity(_cap) {}

        ret(bool _expanded, uint64_t _cap)
            : found(false), bucket_idx(0), step(0), slot_idx(0),
              expanded(_expanded), capacity(_cap) {}

        ret(bool _found)
            : found(_found), bucket_idx(0), step(0), slot_idx(0),
              expanded(false), capacity(0) {}

        ret()
            : found(false), bucket_idx(0), step(0), slot_idx(0),
              expanded(false), capacity(0) {}
    };

    void add_count_write(size_t size) {
#if defined(COUNTING_WRITE)
        /* The lock will be flushed back to PM, so we add a count here */
        pmhb_ns::sample_guard<clht<Key, T>, pmhb_ns::WRITE_COUNT>{size};
#endif
    }


    struct ALIGNED(CACHE_LINE_SIZE) bucket_s {
        clht_lock_t lock;
        volatile uint32_t hops;
        kv_ptr_t slots[ENTRIES_PER_BUCKET];
        bucket_ptr_t next;

        bucket_s() {
            lock = LOCK_FREE;
            for (size_t j; j < ENTRIES_PER_BUCKET; j++) { slots[j] = nullptr; }
            next = nullptr;
        }
    };

    struct ALIGNED(CACHE_LINE_SIZE) clht_hashtable_s {
        union {
            struct {
                size_t num_buckets;
                clht_table_t table;
                size_t hash;
                size_t version;
                uint8_t next_cache_line[CACHE_LINE_SIZE - (3 * sizeof(size_t)) -
                                        (sizeof(clht_table_t))];
                clht_hashtable_ptr_t table_tmp;
                clht_hashtable_ptr_t table_prev;
                clht_hashtable_ptr_t table_new;
                volatile uint32_t num_expands;
                union {
                    volatile uint32_t num_expands_threshold;
                    uint32_t num_buckets_prev;
                };
                volatile int32_t is_helper;
                volatile int32_t helper_done;
                size_t version_min;
            };
            uint8_t padding[2 * CACHE_LINE_SIZE];
        };

        clht_hashtable_s(uint64_t n_buckets = 0) : num_buckets(n_buckets) {
            table = make_persistent<bucket_s[]>(num_buckets);
            hash = num_buckets - 1;
            version = 0;
            table_tmp = nullptr;
            table_new = nullptr;
            table_prev = nullptr;
            num_expands = 0;
            num_expands_threshold = (CLHT_PERC_EXPANSIONS * num_buckets);
            if (num_expands_threshold == 0) { num_expands_threshold = 1; }
            is_helper = 1;
            helper_done = 0;
        }
    };

    struct ALIGNED(CACHE_LINE_SIZE) ht_ts {
        union {
            struct {
                size_t version;
                clht_hashtable_ptr_t versionp;
                int id;
                ht_ts_ptr_t next;
            };
            uint8_t padding[CACHE_LINE_SIZE];
        };
    };

    clht(uint64_t n_buckets) {
        std::cout << "CLHT n_buckets = " << n_buckets << std::endl;

        // setup pool
        PMEMoid oid = pmemobj_oid(this);
        assert(!OID_IS_NULL(oid));
        my_pool_uuid = oid.pool_uuid_lo;
        kv_pool_uuid = oid.pool_uuid_lo;

        persistent_ptr<clht_hashtable_s> ht_tmp =
                make_persistent<clht_hashtable_s>(n_buckets);
        ht.off = ht_tmp.raw().off;

        resize_lock = LOCK_FREE;
        gc_lock = LOCK_FREE;
        status_lock = LOCK_FREE;
        version_list = NULL;
        version_min = 0;
        ht_oldest = ht;
    }

    bucket_ptr_t clht_bucket_create_stats(pool_base &pop,
                                          clht_hashtable_s *ht_ptr,
                                          int &resize) {
        persistent_ptr<bucket_s> tmp;
        make_persistent_atomic<bucket_s>(pop, tmp);

        if (__sync_add_and_fetch(&ht_ptr->num_expands, 1) >=
            ht_ptr->num_expands_threshold)
            resize = 1;
        return bucket_ptr_t(tmp.raw().off);
    }

    /**
	 * Get the persistent memory pool where hashmap resides.
	 * @returns pmem::obj::pool_base object.
	 */
    pool_base get_pool_base() {
        PMEMobjpool *pop = pmemobj_pool_by_oid(PMEMoid{my_pool_uuid, 0});

        return pool_base(pop);
    }

    bool get_lock(pool_base &pop, clht_lock_t *lock, clht_hashtable_s *ht_ptr) {
        uint8_t l;
        while ((l = CLHT_CAS(lock, LOCK_FREE, LOCK_UPDATE)) == LOCK_UPDATE)
            atomic_backoff();// pause for hot path

        if (l == LOCK_RESIZE) {
#if CLHT_HELP_RESIZE == 1
            ht_resize_help(ht_ptr);
#endif
            while (ht_ptr->table_new == nullptr) pop.drain();

            return false;
        }

        return true;
    }

    bool lock_acq_resize(clht_lock_t *lock) {
        uint8_t l;
        while ((l = CLHT_CAS(lock, LOCK_FREE, LOCK_RESIZE)) == LOCK_UPDATE)
            atomic_backoff();// pause for hot path

        if (l == LOCK_RESIZE) return false;

        return true;
    }

    uint8_t try_lock(pool_base &pop, clht_lock_t *lock) {
        return tas_uint8(lock);
    }

    void unlock(pool_base &pop, clht_lock_t *lock) {
        pop.drain();
        *lock = LOCK_FREE;
        add_count_write(sizeof(lock));
    }

    ret get(const key_type &key) const {
#ifdef PMHB_LATENCY
        auto pmhb_guard = pmhb_ns::sample_guard<clht, pmhb_ns::SEARCH>{};
#endif
        hv_type hv = hasher{}(key);
        clht_hashtable_s *ht_ptr = ht.get_address(my_pool_uuid);
        difference_type idx = static_cast<difference_type>(
                hv % static_cast<hv_type>(ht_ptr->num_buckets));
        bucket_s *bucket = &ht_ptr->table[idx];
        uint8_t step = 0;
#ifdef SEARCH_DEBUG
        fmt::print("[S] key: {}, hv: {:x}, idx: {:x}\n Checking", key.begin(),
                   hv, idx);
#endif
        do {
            for (size_t j = 0; j < ENTRIES_PER_BUCKET; j++) {
#ifdef SEARCH_DEBUG
                fmt::print(" {} ", bucket->slots[j] != nullptr
                                           ? bucket->slots[j]
                                                     .get_address(kv_pool_uuid)
                                                     ->first.begin()
                                           : "null");
#endif
                if (bucket->slots[j] != nullptr &&
                    key_equal{}(
                            bucket->slots[j].get_address(kv_pool_uuid)->first,
                            key))
                    return ret(idx, step, j);
            }

            bucket = bucket->next.get_address(my_pool_uuid);
            step++;
        } while (unlikely(bucket != nullptr));

        return ret();
    }

    bool key_exists(bucket_s *bucket, const key_type &key) const {
        do {
            for (size_t j = 0; j < ENTRIES_PER_BUCKET; j++) {
                if (bucket->slots[j] != nullptr &&
                    key_equal{}(
                            bucket->slots[j].get_address(kv_pool_uuid)->first,
                            key))
                    return true;
            }

            bucket = bucket->next.get_address(my_pool_uuid);
        } while (unlikely(bucket != nullptr));
        return false;
    }

    static void allocate_KV_copy_construct(pool_base &pop,
                                           persistent_ptr<value_type> &KV_ptr,
                                           const void *param) {
#ifdef WRITE_KV
        const value_type *v = static_cast<const value_type *>(param);
        internal::make_persistent_object<value_type>(pop, KV_ptr, *v);
#endif
    }

    static void allocate_KV_move_construct(pool_base &pop,
                                           persistent_ptr<value_type> &KV_ptr,
                                           const void *param) {
#ifdef WRITE_KV
        const value_type *v = static_cast<const value_type *>(param);
        internal::make_persistent_object<value_type>(
                pop, KV_ptr, std::move(*const_cast<value_type *>(v)));
#endif
    }

    ret put(const value_type &value, size_type id, size_t off = 0) {
        return generic_insert(value.first, &value, allocate_KV_copy_construct,
                              id, off);
    }

    ret put(value_type &&value, size_type id, size_t off = 0) {
        return generic_insert(value.first, &value, allocate_KV_move_construct,
                              id, off);
    }

    // bool
    ret generic_insert(const key_type &key, const void *param,
                       void (*allocate_KV)(pool_base &,
                                           persistent_ptr<value_type> &,
                                           const void *),
                       size_type id, size_t off) {
#ifdef PMHB_LATENCY
        auto pmhb_guard = pmhb_ns::sample_guard<clht, pmhb_ns::INSERT>{};
#endif
        pool_base pop = get_pool_base();
        persistent_ptr<value_type> tmp_entry;
        if (off == 0) {
#ifdef WRITE_KV
            allocate_KV(pop, tmp_entry, param);
            add_count_write(sizeof(value_type));
#endif
        } else {
            tmp_entry = PMEMoid{kv_pool_uuid, off};
        }

        clht_hashtable_s *ht_ptr = ht.get_address(my_pool_uuid);
        hv_type hv = hasher{}(key);
        difference_type idx = static_cast<difference_type>(
                hv % static_cast<hv_type>(ht_ptr->num_buckets));
        bucket_s *bucket = &ht_ptr->table[idx];

        bool expanded = false;
        uint64_t initial_capacity = 0;

#ifdef DEBUG_RESIZING
        initial_capacity = capacity();
#endif

#if CLHT_READ_ONLY_FAIL == 1
        if (key_exists(bucket, key)) return ret(true);
#endif

        clht_lock_t *lock = &bucket->lock;
        while (!get_lock(pop, lock, ht_ptr)) {
            ht_ptr = ht.get_address(my_pool_uuid);
            idx = static_cast<difference_type>(
                    hv % static_cast<hv_type>(ht_ptr->num_buckets));
            bucket = &ht_ptr->table[idx];
            lock = &bucket->lock;
        }

        kv_ptr_t *empty = nullptr;
        do {
            for (size_t j = 0; j < ENTRIES_PER_BUCKET; j++) {
                if (bucket->slots[j] != nullptr &&
                    key_equal{}(
                            bucket->slots[j].get_address(kv_pool_uuid)->first,
                            key)) {
                    unlock(pop, lock);
                    return ret(true);
                } else if (empty == nullptr && bucket->slots[j] == nullptr)
                    empty = &bucket->slots[j];
            }

            int resize = 0;
            if (likely(bucket->next == nullptr)) {
                if (unlikely(empty == nullptr)) {
                    bucket_ptr_t b_new =
                            clht_bucket_create_stats(pop, ht_ptr, resize);
                    kv_ptr_t &s_new = b_new(my_pool_uuid)->slots[0];

                    s_new.off = tmp_entry.raw().off;
                    pop.persist(&s_new.off, sizeof(kv_ptr_t));
                    add_count_write(sizeof(kv_ptr_t));
                    bucket->next = b_new;
                    pop.persist(&bucket->next.off, sizeof(bucket_ptr_t));
                    add_count_write(sizeof(bucket_ptr_t));
                } else {
                    empty->off = tmp_entry.raw().off;
                    pop.persist(&empty->off, sizeof(kv_ptr_t));
                    add_count_write(sizeof(kv_ptr_t));
                }

                unlock(pop, lock);
                if (unlikely(resize)) {
#ifdef DEBUG_RESIZING
                    expanded = true;
#endif
                    // Start resizing... If crash, return true, because
                    // the insert anyway succeeded
                    if (ht_status(true /* is_increase */,
                                  false /* just_print */) == 0)
                        return ret(expanded, initial_capacity);
                }
                return ret(expanded, initial_capacity);
            }
            bucket = bucket->next.get_address(my_pool_uuid);
        } while (true);
    }

    ret update(const key_type &key, size_type id, size_t off) {
#ifdef PMHB_LATENCY
        auto pmhb_guard = pmhb_ns::sample_guard<clht, pmhb_ns::DELETE>{};
#endif
        pool_base pop = get_pool_base();
        hv_type hv = hasher{}(key);
        clht_hashtable_s *ht_ptr = ht.get_address(my_pool_uuid);
        difference_type idx = static_cast<difference_type>(
                hv % static_cast<hv_type>(ht_ptr->num_buckets));
        bucket_s *bucket = &ht_ptr->table[idx];
        uint8_t step = 0;

#if CLHT_READ_ONLY_FAIL == 1
        if (!key_exists(bucket, key)) return ret();
#endif

#ifdef UPDATE_DEBUG


#endif

        clht_lock_t *lock = &bucket->lock;
        while (!get_lock(pop, lock, ht_ptr)) {
            ht_ptr = ht.get_address(my_pool_uuid);
            idx = static_cast<difference_type>(
                    hv % static_cast<hv_type>(ht_ptr->num_buckets));
            bucket = &ht_ptr->table[idx];
            lock = &bucket->lock;
        }

        do {
            for (size_t j = 0; j < ENTRIES_PER_BUCKET; j++) {
                if (bucket->slots[j] != nullptr &&
                    key_equal{}(
                            bucket->slots[j].get_address(kv_pool_uuid)->first,
                            key)) {
                    bucket->slots[j].off = off;
                    pop.persist(&bucket->slots[j].off, sizeof(kv_ptr_t));
                    add_count_write(sizeof(kv_ptr_t));

                    unlock(pop, lock);
                    return ret(idx, step, j);
                }
            }

            bucket = bucket->next.get_address(my_pool_uuid);
            step++;
        } while (unlikely(bucket != nullptr));

        unlock(pop, lock);
        return ret();
    }

    ret erase(const key_type &key) {
#ifdef PMHB_LATENCY
        auto pmhb_guard = pmhb_ns::sample_guard<clht, pmhb_ns::DELETE>{};
#endif
        pool_base pop = get_pool_base();
        hv_type hv = hasher{}(key);
        clht_hashtable_s *ht_ptr = ht.get_address(my_pool_uuid);
        difference_type idx = static_cast<difference_type>(
                hv % static_cast<hv_type>(ht_ptr->num_buckets));
        bucket_s *bucket = &ht_ptr->table[idx];
        uint8_t step = 0;

#if CLHT_READ_ONLY_FAIL == 1
        if (!key_exists(bucket, key)) return ret();
#endif

        clht_lock_t *lock = &bucket->lock;
        while (!get_lock(pop, lock, ht_ptr)) {
            ht_ptr = ht.get_address(my_pool_uuid);
            idx = static_cast<difference_type>(
                    hv % static_cast<hv_type>(ht_ptr->num_buckets));
            bucket = &ht_ptr->table[idx];
            lock = &bucket->lock;
        }

        do {
            for (size_t j = 0; j < ENTRIES_PER_BUCKET; j++) {
                if (bucket->slots[j] != nullptr &&
                    key_equal{}(
                            bucket->slots[j].get_address(kv_pool_uuid)->first,
                            key)) {
                    PMEMoid oid = bucket->slots[j].raw_ptr(kv_pool_uuid);
#ifdef WRITE_KV
                    pmemobj_free(&oid);
#endif
                    bucket->slots[j] = nullptr;
                    pop.persist(&bucket->slots[j].off, sizeof(kv_ptr_t));
                    add_count_write(sizeof(kv_ptr_t));

                    unlock(pop, lock);
                    return ret(idx, step, j);
                }
            }

            bucket = bucket->next.get_address(my_pool_uuid);
            step++;
        } while (unlikely(bucket != nullptr));

        unlock(pop, lock);
        return ret();
    }

    size_type size() {
        clht_hashtable_s *ht_ptr = ht.get_address(my_pool_uuid);
        uint64_t n_buckets = ht_ptr->num_buckets;
        bucket_s *bucket = nullptr;
        size_type size = 0;

        for (difference_type idx = 0; idx < n_buckets; idx++) {
            bucket = ht_ptr->table[idx].get_address(my_pool_uuid);
            do {
                for (size_t j = 0; j < ENTRIES_PER_BUCKET; j++) {
                    if (bucket->slots[j] != nullptr) size++;
                }

                bucket = bucket->next.get_address(my_pool_uuid);
            } while (unlikely(bucket != nullptr));
        }

        return size;
    }

    size_type ht_status(bool is_increase, bool just_print) {
        pool_base pop = get_pool_base();
        if (try_lock(pop, &status_lock) && !is_increase) return 0;

        clht_hashtable_s *ht_ptr = ht.get_address(my_pool_uuid);
        difference_type n_buckets = (difference_type) ht_ptr->num_buckets;
        bucket_s *bucket = nullptr;
        size_type size = 0;
        int expands = 0;
        int expands_max = 0;

        for (difference_type idx = 0; idx < n_buckets; idx++) {
            bucket = &ht_ptr->table[idx];

            int expands_cont = -1;
            expands--;
            do {
                expands_cont++;
                expands++;
                for (size_t j = 0; j < ENTRIES_PER_BUCKET; j++) {
                    if (bucket->slots[j] != nullptr) size++;
                }

                bucket = bucket->next.get_address(my_pool_uuid);
            } while (unlikely(bucket != nullptr));

            if (expands_cont > expands_max) expands_max = expands_cont;
        }

        double full_ratio = 100.0 * size / (n_buckets * ENTRIES_PER_BUCKET);

        if (just_print) {
            printf("[STATUS-%02d] #bu: %7zu / #elems: %7zu / full%%: %8.4f%% / "
                   "expands: %4d / max expands: %2d\n",
                   99, n_buckets, size, full_ratio, expands, expands_max);
        } else {
            if (full_ratio > 0 && full_ratio < CLHT_PERC_FULL_HALVE) {
                ht_resize_pes(false /* is_increase */, (size_t) 33);
            } else if ((full_ratio > 0 && full_ratio > CLHT_PERC_FULL_DOUBLE) ||
                       expands_max > CLHT_MAX_EXPANSIONS || is_increase) {
                uint64_t inc_by =
                        (uint64_t) (full_ratio / CLHT_OCCUP_AFTER_RES);
                int inc_by_pow2 = pow2roundup(inc_by);

                if (inc_by_pow2 == 1) { inc_by_pow2 = 2; }
                // fmt::print("inc times {}\n", (size_t) inc_by_pow2);
                int ret = ht_resize_pes(true /* is_increase */,
                                        (size_t) inc_by_pow2);
                // return if crashed
                if (ret == -1) return 0;
            }
        }

        unlock(pop, &status_lock);
        return size;
    }

    /// Round up to next higher power of 2 (return x if it's already a power
    /// of 2) for 32-bit numbers
    static inline uint64_t pow2roundup(uint64_t x) {
        if (x == 0) return 1;
        --x;
        x |= x >> 1;
        x |= x >> 2;
        x |= x >> 4;
        x |= x >> 8;
        x |= x >> 16;
        x |= x >> 32;
        return x + 1;
    }

    /**
     * Perform actual resizing.
    */
    int ht_resize_pes(bool is_increase, size_t by) {
#ifdef PMHB_LATENCY
        auto pmhb_guard = pmhb_ns::sample_guard<clht, pmhb_ns::DOUBLE>{};
#endif
        pool_base pop = get_pool_base();

        clht_hashtable_s *ht_old = ht.get_address(my_pool_uuid);
        if (try_lock(pop, &resize_lock)) return 0;

        size_t num_buckets_new;
        if (is_increase) {
            /* num_buckets_new = CLHT_RATIO_DOUBLE * ht_old->num_buckets; */
            num_buckets_new = by * ht_old->num_buckets;
        } else {
#if CLHT_HELP_RESIZE == 1
            ht_old->is_helper = 0;
#endif
            num_buckets_new = ht_old->num_buckets / CLHT_RATIO_HALVE;
        }
        fmt::print("Resizing\n");

        persistent_ptr<clht_hashtable_s> ht_tmp;
        transaction::run(pop, [&] {
            // A transaction is required as the constructor of clht_hashtable_s
            // invokes "make_persistent".
            ht_tmp = make_persistent<clht_hashtable_s>(num_buckets_new);
        });

        clht_hashtable_s *ht_new = ht_tmp.get();
        ht_new->version = ht_old->version + 1;

#if CLHT_HELP_RESIZE == 1
        ht_old->table_tmp.off = ht_tmp.raw().off;
        pop.persist(&ht_old->table_tmp.off, sizeof(clht_hashtable_ptr_t));
        add_count_write(sizeof(clht_hashtable_ptr_t));

        for (difference_type idx = 0;
             idx < (difference_type) ht_old->num_buckets; idx++) {
            bucket_s *bu_cur = &ht_old->table[idx];
            int ret = bucket_cpy(bu_cur, ht_new);
            /* reached a point where the helper is handling */
            if (ret == -1) return -1;

            if (!ret) break;
        }

        if (is_increase && ht_old->is_helper != 1) /* there exist a helper */
        {
            while (ht_old->helper_done != 1) atomic_backoff();
        }
#else
        for (difference_type idx = 0;
             idx < (difference_type) ht_old->num_buckets; idx++) {
            bucket_s *bu_cur = &ht_old->table[idx];
            int ret = bucket_cpy(bu_cur, ht_new);
            if (ret == -1) return -1;
        }
#endif

        ht_new->table_prev = ht;
        int ht_resize_again = 0;
        if (ht_new->num_expands >= ht_new->num_expands_threshold) {
            ht_resize_again = 1;
        }

        pop.drain();

        // Switch to the new hash table
        ht.off = ht_tmp.raw().off;
        pop.persist(&ht.off, sizeof(clht_hashtable_ptr_t));
        add_count_write(sizeof(clht_hashtable_ptr_t));
        ht_old->table_new.off = ht_tmp.raw().off;
        pop.persist(&ht_old->table_new.off, sizeof(clht_hashtable_ptr_t));
        add_count_write(sizeof(clht_hashtable_ptr_t));

        unlock(pop, &resize_lock);

        if (ht_resize_again)
            ht_status(true /* is_increase */, false /* just_print */);

        return 1;
    }

    int bucket_cpy(bucket_s *bucket, clht_hashtable_s *ht_new) {
        if (!lock_acq_resize(&bucket->lock)) return 0;

        size_t involved_kv = 0;
        do {
            for (size_t j = 0; j < ENTRIES_PER_BUCKET; j++) {
                if (bucket->slots[j] != nullptr) {
                    hv_type hv = hasher{}(
                            bucket->slots[j].get_address(kv_pool_uuid)->first);
                    difference_type idx = static_cast<difference_type>(
                            hv % static_cast<hv_type>(ht_new->num_buckets));
#ifdef SEARCH_DEBUG
                    fmt::print("Rehash {} hv {:x} to bucket idx: "
                               "{:x} (%{})\n",
                               bucket->slots[j]
                                       .get_address(kv_pool_uuid)
                                       ->first.begin(),
                               hv, idx,
                               static_cast<hv_type>(ht_new->num_buckets));
#endif
                    put_seq(ht_new, bucket->slots[j], idx);
                    involved_kv++;
                }
            }
            bucket = bucket->next.get_address(my_pool_uuid);
        } while (unlikely(bucket != nullptr));
#ifdef PMHB_LATENCY
        pmhb_ns::sample_guard<clht, pmhb_ns::RESIZE_ITEM_NUMBER>{involved_kv};
        /* Lock */
#endif

        return 1;
    }

    bool put_seq(clht_hashtable_s *hashtable, kv_ptr_t slot,
                 difference_type idx) {
        pool_base pop = get_pool_base();
        bucket_s *bucket = &hashtable->table[idx];

        do {
            for (size_t j = 0; j < ENTRIES_PER_BUCKET; j++) {
                if (bucket->slots[j] == nullptr) {
                    bucket->slots[j] = slot;
#ifdef SEARCH_DEBUG
                    fmt::print("Rehash {} to new bucket idx: {:x} \n",
                               bucket->slots[j]
                                       .get_address(kv_pool_uuid)
                                       ->first.begin(),
                               idx);
#endif
                    pop.persist(&bucket->slots[j].off, sizeof(kv_ptr_t));
                    add_count_write(sizeof(kv_ptr_t));
                    return true;
                }
#ifdef SEARCH_DEBUG
                else
                    fmt::print("Rehash encounter {} ",
                               bucket->slots[j]
                                       .get_address(kv_pool_uuid)
                                       ->first.begin());
#endif
            }

            if (bucket->next == NULL) {
                int null;
                bucket_ptr_t b_new =
                        clht_bucket_create_stats(pop, hashtable, null);
                kv_ptr_t &s_new = b_new(my_pool_uuid)->slots[0];
                s_new.off = slot.off;
#ifdef SEARCH_DEBUG
                fmt::print("Rehash {} to newly created bucket idx: {:x} \n",
                           s_new.get_address(kv_pool_uuid)->first.begin(), idx);
#endif
                pop.persist(&s_new.off, sizeof(kv_ptr_t));
                add_count_write(sizeof(kv_ptr_t));
                bucket->next = b_new;
                pop.persist(&bucket->next.off, sizeof(bucket_ptr_t));
                return true;
            }

            bucket = bucket->next.get_address(my_pool_uuid);
        } while (true);
    }

    void ht_resize_help(clht_hashtable_s *hashtable) {
        if ((int32_t) __sync_fetch_and_sub(
                    (volatile uint32_t *) &hashtable->is_helper, 1) <= 0)
            return;

        for (difference_type idx = (difference_type) hashtable->hash; idx >= 0;
             idx--) {
            bucket_s *bucket = &hashtable->table[idx];
            if (!bucket_cpy(bucket,
                            hashtable->table_tmp.get_address(my_pool_uuid)))
                break;
        }

        hashtable->helper_done = 1;
    }

    void print() {
        clht_hashtable_s *hashtable = ht.get_address(my_pool_uuid);
        difference_type n_buckets = (difference_type) hashtable->num_buckets;
        std::cout << "Number of buckets: " << n_buckets << std::endl;

        for (difference_type idx = 0; idx < n_buckets; idx++) {
            bucket_s *bucket = &hashtable->table[idx];
            std::cout << "[[" << idx << "]]";

            do {
                for (size_t j = 0; j < ENTRIES_PER_BUCKET; j++) {
                    if (bucket->slots[j] != nullptr) {
                        value_type *e =
                                bucket->slots[j].get_address(kv_pool_uuid);
                        std::cout << "(" << e->first << "/" << e.second
                                  << ")-> ";
                    }
                }

                bucket = bucket->next.get_address(my_pool_uuid);
                std::cout << " ** -> ";
            } while (unlikely(bucket != nullptr));
            std::cout << std::endl;
        }

        fflush(stdout);
    }

    uint64_t capacity() {
        clht_hashtable_s *hashtable = ht.get_address(my_pool_uuid);
        difference_type n_buckets = (difference_type) hashtable->num_buckets;

        uint64_t num = 0;
        for (difference_type idx = 0; idx < n_buckets; idx++) {
            bucket_s *bucket = &hashtable->table[idx];
            do {
                num++;
                bucket = bucket->next.get_address(my_pool_uuid);
            } while (unlikely(bucket != nullptr));
        }

        return num * ENTRIES_PER_BUCKET;
    }

    size_t get_memory_usage() {
        clht_hashtable_s *hashtable = ht.get_address(my_pool_uuid);
        difference_type n_buckets = (difference_type) hashtable->num_buckets;
        return n_buckets * sizeof(bucket_s) + sizeof(clht_hashtable_s);
    }


    clht_hashtable_ptr_t ht;
    uint8_t next_cache_line[CACHE_LINE_SIZE - (sizeof(clht_hashtable_ptr_t))];
    clht_hashtable_ptr_t ht_oldest;
    ht_ts_ptr_t version_list;
    size_t version_min;
    clht_lock_t resize_lock;
    clht_lock_t gc_lock;
    clht_lock_t status_lock;

    /** ID of persistent memory pool where hash map resides. */
    p<uint64_t> my_pool_uuid;
    size_t kv_pool_uuid;
};

} /* namespace pclht_ns */

#endif /* PMEMOBJ_CLHT_HPP */