
// Copyright (c) Simon Fraser University & The Chinese University of Hong Kong. All rights reserved.
// Licensed under the MIT license.
#ifndef ALLOC_DASH_H
#define ALLOC_DASH_H

#include <filesystem>
#include <sys/mman.h>

#include "x86intrin.h"
#include <libpmem.h>
#include <libpmemobj.h>
#include <sched.h>

#if defined PMHB_LATENCY || defined COUNTING_WRITE
#include "../../include/sample_guard.hpp"
#endif

TOID_DECLARE(char, 123);

#define PMEM

#define LOG_FATAL_DASH(msg)                                                    \
    std::cout << msg << "\n";                                                  \
    exit(-1)

#define LOG_DASH(msg) std::cout << msg << "\n"


namespace dash_ns {
inline static const char *layout_name = "hashtable";

template<class KV>
class Finger_EH;
void pre_fault(void *pm, size_t len, size_t granularity = 2ul << 20) {
#ifdef PREFAULT
    auto light_pin_thread = [](size_t id) {
        cpu_set_t mask;
        cpu_set_t get;
        CPU_ZERO(&mask);
        CPU_SET(id, &mask);
        if (sched_setaffinity(0, sizeof(mask), &mask) == -1) {
            printf("[FAIL] cannot set affinity\n");
        }

        CPU_ZERO(&get);
        if (sched_getaffinity(0, sizeof(get), &get) == -1) {
            printf("[FAIL] cannot get affinity\n");
        }
    };
    auto sub_worker = [light_pin_thread](size_t id, unsigned char *s_base,
                                         size_t s_len, size_t s_granularity) {
        light_pin_thread(id);
        for (size_t i = 0; i < s_len; i += s_granularity) {
            ((unsigned char volatile *) s_base)[i] =
                    ((unsigned char volatile *) s_base)[i];
        }
    };
    unsigned char *base = (unsigned char *) pm;
    const size_t thread_num = 24;
    std::thread th[thread_num];
    for (size_t i = 0; i < thread_num; i++) {
        th[i] = std::thread(sub_worker, i, base + i * (len / thread_num),
                            len / thread_num, granularity);
    }
    for (size_t i = 0; i < thread_num; i++) { th[i].join(); }
#endif
    return;
}
typedef void (*DestroyCallback)(void *callback_context, void *object);

struct Allocator {
public:
#ifdef PMEM
    static void Initialize(const char *pool_name, size_t pool_size);

    static void Close_pool();

    static void ReInitialize_test_only(const char *pool_name, size_t pool_size);

    Allocator(const char *pool_name, size_t pool_size);

    PMEMobjpool *pm_pool_{nullptr};
    // EpochManager epoch_manager_{};
    // GarbageList garbage_list_{};

    static Allocator *instance_;
    static Allocator *Get();

    /* Must ensure that this pointer is in persistent memory*/
    static void Allocate(void **ptr, uint32_t alignment, size_t size,
                         int (*alloc_constr)(PMEMobjpool *pool, void *ptr,
                                             void *arg),
                         void *arg);

    static void Allocate(PMEMoid *pm_ptr, uint32_t alignment, size_t size,
                         int (*alloc_constr)(PMEMobjpool *pool, void *ptr,
                                             void *arg),
                         void *arg);

    static void *GetRoot(size_t size);

    static void Persist(void *ptr, size_t size);

    template<typename KV>
    static void Persist(void *ptr, size_t size);

    static void NTWrite64(uint64_t *ptr, uint64_t val);

    static void NTWrite32(uint32_t *ptr, uint32_t val);

    static PMEMobjpool *GetPool();

#endif

    // static void Allocate(void **ptr, uint32_t alignment, size_t size);

    /*Must ensure that this pointer is in persistent memory*/
    static void ZAllocate(void **ptr, uint32_t alignment, size_t size);

    static void ZAllocate(PMEMoid *pm_ptr, uint32_t alignment, size_t size);

    static void DefaultCallback(void *callback_context, void *ptr);

    static void Free(void *ptr);
};


#ifdef PMEM
Allocator *Allocator::instance_ = nullptr;
void Allocator::Initialize(const char *pool_name, size_t pool_size) {
    instance_ = new Allocator(pool_name, pool_size);
    std::cout << "pool opened at: " << std::hex << instance_->pm_pool_
              << std::dec << std::endl;
    std::cout << "Going out" << std::endl;
}
void Allocator::Close_pool() {
    auto pool = instance_->pm_pool_;
    // the address space must be present while calling dtor
    delete instance_;
    pmemobj_close(pool);
}
void Allocator::ReInitialize_test_only(const char *pool_name,
                                       size_t pool_size) {
    pmemobj_close(instance_->pm_pool_);
    delete instance_;
    Allocator::Initialize(pool_name, pool_size);
}
Allocator::Allocator(const char *pool_name, size_t pool_size) {
    if (!std::filesystem::exists(pool_name)) {
        LOG_DASH("creating a new pool");
        pm_pool_ = pmemobj_create(pool_name, layout_name, pool_size, 0600);
        auto ret = pmemobj_direct(pmemobj_root(pm_pool_, sizeof(int)));
        /* To eliminate page fault for the main pool */
        pre_fault(pm_pool_, pool_size);
        // pre_fault(ret, pool_size);
        if (pm_pool_ == nullptr) { LOG_FATAL_DASH("failed to create a pool;"); }
    } else {
        LOG_DASH("opening an existing pool, and trying to map to same address");
        /* Need to open an existing persistent pool */
        pm_pool_ = pmemobj_open(pool_name, layout_name);
        if (pm_pool_ == nullptr) { LOG_FATAL_DASH("failed to open the pool"); }
    }
}
Allocator *Allocator::Get() { return instance_; }
void Allocator::Allocate(void **ptr, uint32_t alignment, size_t size,
                         int (*alloc_constr)(PMEMobjpool *, void *, void *),
                         void *arg) {
    TX_BEGIN(instance_->pm_pool_) {
        pmemobj_tx_add_range_direct(ptr, sizeof(*ptr));
        *ptr = pmemobj_direct(pmemobj_tx_alloc(size, TOID_TYPE_NUM(char)));
        alloc_constr(instance_->pm_pool_, *ptr, arg);
    }
    TX_ONABORT { LOG_FATAL_DASH("Allocate: TXN Allocation Error"); }
    TX_END
}
void Allocator::Allocate(PMEMoid *pm_ptr, uint32_t alignment, size_t size,
                         int (*alloc_constr)(PMEMobjpool *, void *, void *),
                         void *arg) {
    auto ret = pmemobj_alloc(instance_->pm_pool_, pm_ptr, size,
                             TOID_TYPE_NUM(char), alloc_constr, arg);
    if (ret) { LOG_FATAL_DASH("Allocate: Allocation Error in PMEMoid"); }
}
void *Allocator::GetRoot(size_t size) {
    return pmemobj_direct(pmemobj_root(instance_->pm_pool_, size));
}
void Allocator::Persist(void *ptr, size_t size) {
    pmemobj_persist(instance_->pm_pool_, ptr, size);
}

template<typename KV>
void Allocator::Persist(void *ptr, size_t size) {
    pmemobj_persist(instance_->pm_pool_, ptr, size);
#if defined(COUNTING_WRITE)
    pmhb_ns::sample_guard<Finger_EH<KV>, pmhb_ns::WRITE_COUNT>{size};
#endif
}

void Allocator::NTWrite64(uint64_t *ptr, uint64_t val) {
    _mm_stream_si64((long long *) ptr, val);
}
void Allocator::NTWrite32(uint32_t *ptr, uint32_t val) {
    _mm_stream_si32((int *) ptr, val);
}
PMEMobjpool *Allocator::GetPool() { return instance_->pm_pool_; }

// void Allocator::Allocate(void **ptr, uint32_t alignment, size_t size) {
//     posix_memalign(ptr, alignment, size);
// }
// void Allocator::ZAllocate(void **ptr, uint32_t alignment, size_t size) {
// #ifdef PMEM
//     TX_BEGIN(instance_->pm_pool_) {
//         pmemobj_tx_add_range_direct(ptr, sizeof(*ptr));
//         *ptr = pmemobj_direct(pmemobj_tx_zalloc(size, TOID_TYPE_NUM(char)));
//     }
//     TX_ONABORT { LOG_FATAL_DASH("ZAllocate: TXN Allocation Error"); }
//     TX_END
// #else
//     posix_memalign(ptr, alignment, size);
//     memset(*ptr, 0, size);
// #endif
// }
void Allocator::ZAllocate(PMEMoid *pm_ptr, uint32_t alignment, size_t size) {
    auto ret = pmemobj_zalloc(instance_->pm_pool_, pm_ptr, size,
                              TOID_TYPE_NUM(char));

    if (ret) {
        std::cout << "Allocation size = " << size << std::endl;
        LOG_FATAL_DASH("allocation error");
    }
}
void Allocator::DefaultCallback(void *callback_context, void *ptr) {
#ifdef PMEM
    auto oid_ptr = pmemobj_oid(ptr);
    TOID(char) ptr_cpy;
    TOID_ASSIGN(ptr_cpy, oid_ptr);
    POBJ_FREE(&ptr_cpy);
#else
    free(ptr);
#endif
}
void Allocator::Free(void *ptr) {
    /* get the oid, and free */
    PMEMoid oid = pmemobj_oid(ptr);
    pmemobj_free(&oid);
}
#endif

}// namespace dash_ns

#endif