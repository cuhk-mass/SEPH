#ifndef STEPH_SUBSTRUCTURE_HPP
#define STEPH_SUBSTRUCTURE_HPP

#include "alloc.hpp"
#include "config.hpp"
#include "util.hpp"

#include <array>
#include <atomic>
#include <deque>
#include <immintrin.h>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/make_persistent_atomic.hpp>
#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/transaction.hpp>
#include <libpmemobj++/utils.hpp>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>


namespace steph_ns {

template<typename KV>
struct kv_ptr;
template<typename KV>
struct segment_ptr;
template<typename KV>
struct c_ptr;
template<typename KV>
struct steph;

enum { SUCCESS, FAIL, RETRY };

template<typename KV>
struct Bucket {
    /* Data members */
    std::array<kv_ptr<KV>, KV_NUM_PER_BUCKET> slots;
#ifdef TRADITIONAL_LOCK
    std::atomic<size_t> b_lock = 0;
#endif

    /* Constructors */
    Bucket() = default;

    /* Interfaces */
#ifdef TRADITIONAL_LOCK
    bool lock() {
        size_t expected = 0;
        return b_lock.compare_exchange_strong(expected, 1);
    }

    void unlock() { b_lock.store(0); }
#endif
    void persist_neighbors(size_t slot_idx) {
/* To tune the performance, try to persist the XPline or Cache line */
#ifndef NO_DIRTY_FLAG
        constexpr size_t stride = 8;
        size_t slot_idx_base = slot_idx & (~(stride - 1));
        pmem_persist(&slots[slot_idx_base], sizeof(kv_ptr<KV>) * stride);

        for (size_t i = 0; i < stride; i++) {
            auto &slot = slots[slot_idx_base + i];
            if (slot == nullptr) break;
            if (slot.is_volatile()) { slot.clear_dirty_flag(); }
        }
        pmem_persist(&slots[slot_idx_base], stride * sizeof(kv_ptr<KV>));
        add_write_counter<KV>(stride * sizeof(kv_ptr<KV>));
#endif
    }
    void persist_line() {
#ifndef NO_DIRTY_FLAG
        Bucket slots_snapshot;
        slots_snapshot.slots = slots;
        pmem_persist(&slots, sizeof(slots));
        add_write_counter<KV>(sizeof(slots));

        for (size_t i = 0; i < KV_NUM_PER_BUCKET; i++) {
            auto &slot = slots[i];
            if (slots[i] == nullptr) {
                break;
            } else {
                if (slot.is_volatile()) slot.clear_for(slots_snapshot.slots[i]);
            }
        }
        pmem_persist(&slots, sizeof(slots));
        add_write_counter<KV>(sizeof(slots));
#endif
    }
};


template<typename KV>
struct Segment {
    /* Data members */
    std::array<Bucket<KV>, BUCKET_NUM_PER_SEGMENT> buckets;
    inline static stack_allocator<Segment> *allocator{nullptr};

    /* Constructors */
    Segment() = default;

    /* Interfaces */
    KV *iterative_search(std::string_view k, size_t fingerprint, size_t bidx) {
        auto &bucket = buckets[bidx];
        size_t i = 0;
        for (auto &slot : bucket.slots) {
            i++;
            kv_ptr local_slot = slot;
            if (local_slot == nullptr) {
#ifdef DEBUG
                myLOG_DEBUG("null in search with {:x}\n", local_slot.data);
#endif
                return nullptr;
            } else if (local_slot.authenticate(fingerprint, slot) &&
                       local_slot->key() == k) {
#ifdef DEBUG
                myLOG_DEBUG("get in search - equal-{}: key {}\n",
                            local_slot->key() == k, slot.get()->key());
#endif
                if (slot.is_volatile()) {
                    /* Clear the dirty bit before the value return */
                    /* to avoid the dependent data is volatile */
#ifdef BATCH_PERSIST
                    buckets[bidx].persist_line();
#else
                    slot.persist_and_clear();
#endif
                }

                return local_slot.get();
            } else {
#ifdef DEBUG
                myLOG_DEBUG("probing {} with {:04x} {}, provided {:04x}\n",
                            local_slot->key(), (size_t) local_slot.fingerprint,
                            (bool) local_slot.stale, fingerprint);
#endif
            }
        }
        return nullptr;
    }
    std::tuple<KV *, kv_ptr<KV> *, bool> avx_search(std::string_view k,
                                                    size_t fresh_fingerprint,
                                                    size_t stale_fingerprint,
                                                    size_t bidx, int mode = 0) {

#ifdef DEBUG
        std::string detected_keys;
        fmt::print("detected_keys in bidx {} in {} with freshFP {:04x} and "
                   "staleFP "
                   "{:04x}:",
                   bidx, mode ? "insert" : "search", fresh_fingerprint,
                   stale_fingerprint);
        for (size_t j = 0; j < KV_NUM_PER_BUCKET; j++) {
            if (buckets[bidx].slots[j] != nullptr) {
                if (buckets[bidx].slots[j].get()->key() == k) {
                    fmt::print("FOUND KEY {} with info {}\n", k,
                               buckets[bidx].slots[j].information());
                    // detected_keys.clear();
                    // break;
                }
                detected_keys += fmt::format(
                        "{}({:04x}) ", buckets[bidx].slots[j].get()->key(),
                        (int) buckets[bidx].slots[j].fingerprint);
            }
        }
        if (!detected_keys.empty()) {
            fmt::print("{}", detected_keys.c_str());
            fmt::print("\n");
        }

        fmt::print("Detechted Done\n");
#endif
        constexpr size_t summary_mask = 0xffff'8000'0000'0000;
        auto summary_masks = _mm512_set1_epi64(summary_mask);
#ifdef ZERO_BREAK
        auto zero_masks = _mm512_set1_epi64(0);
#endif
        auto fresh_fps = _mm512_set1_epi64(fresh_fingerprint << 48);
        auto stale_fps = _mm512_set1_epi64((stale_fingerprint << 48) |
                                           0x0000'8000'0000'0000);
#ifndef TRADITIONAL_LOCK
        auto copied_masks = _mm512_set1_epi64(kv_ptr<KV>::COPIED_FLAG_MASK);
#endif

        /* Check data summary (fingerprint + stale) with _mm512_cmpeq_epi64_mask */
        for (size_t i = 0; i < KV_NUM_PER_BUCKET; i += 8) {
            auto slot_v = _mm512_stream_load_si512(&buckets[bidx].slots[i]);
            char empty = _mm512_cmpeq_epi64_mask(slot_v, zero_masks);
            auto summary = _mm512_and_epi64(slot_v, summary_masks);
            char result = _mm512_cmpeq_epi64_mask(summary, fresh_fps) |
                          _mm512_cmpeq_epi64_mask(summary, stale_fps);

#ifndef TRADITIONAL_LOCK
            auto copied = _mm512_cmpeq_epi64_mask(slot_v, copied_masks);
            if (copied) { return {nullptr, nullptr, true}; }
#endif


#ifdef TRADITIONAL_LOCK
            result &= ~(1ul << 63);
            empty &= ~(1ul << 63);
#endif
            while (result) [[unlikely]] {
                    int idx = __builtin_ffs(result);
                    auto local_slot = buckets[bidx].slots[i + idx - 1];
                    auto &slot = buckets[bidx].slots[i + idx - 1];
                    if (local_slot.is_tombstone()) [[unlikely]] {
                        if (local_slot.is_volatile()) {
#ifdef BATCH_PERSIST
                            buckets[bidx].persist_line();
#else
                            slot.persist_and_clear();
#endif
                        }
                        result &= ~(1 << (idx - 1));
                        continue;
                    }
                    if (local_slot == nullptr || local_slot->key() != k) {
                        result &= ~(1 << (idx - 1));
                        // myLOG_DEBUG("idx = {}, result = {:x}\n", idx, result);
                        continue;
                    }
                    if (local_slot.is_volatile()) {
                        /* Clear the dirty bit before the value return */
                        /* to avoid the dependent value is volatile */
                        // fmt::print("*");
#ifdef BATCH_PERSIST
                        buckets[bidx].persist_line();
#else
                        slot.persist_and_clear();
#endif
                    }
                    return {local_slot.get(), nullptr, false};
                }
#ifdef ZERO_BREAK
            if (empty) {
                int idx = __builtin_ffs(empty);
                auto &slot = buckets[bidx].slots[i + idx - 1];
                /* nullptr shows up */
                return {nullptr, &slot, false};
            }
#endif
        }
        return {nullptr, nullptr, false};
    }

    kv_ptr<KV> *avx_find_slot(std::string_view k, size_t fresh_fingerprint,
                              size_t stale_fingerprint, size_t bidx) {

        constexpr size_t summary_mask = 0xffff'8000'0000'0000;
        auto summary_masks = _mm512_set1_epi64(summary_mask);
#ifdef ZERO_BREAK
        auto zero_masks = _mm512_set1_epi64(0);
#endif
        auto fresh_fps = _mm512_set1_epi64(fresh_fingerprint << 48);
        auto stale_fps = _mm512_set1_epi64((stale_fingerprint << 48) |
                                           0x0000'8000'0000'0000);

        /* Check data summary (fingerprint + stale) with _mm512_cmpeq_epi64_mask */
        for (size_t i = 0; i < KV_NUM_PER_BUCKET; i += 8) {
            auto slot_v = _mm512_stream_load_si512(&buckets[bidx].slots[i]);
            char empty = _mm512_cmpeq_epi64_mask(slot_v, zero_masks);
            auto summary = _mm512_and_epi64(slot_v, summary_masks);
            char result = _mm512_cmpeq_epi64_mask(summary, fresh_fps) |
                          _mm512_cmpeq_epi64_mask(summary, stale_fps);

            while (result) [[unlikely]] {
                    int idx = __builtin_ffs(result);
                    auto local_slot = buckets[bidx].slots[i + idx - 1];
                    auto &slot = buckets[bidx].slots[i + idx - 1];
                    if (local_slot.is_tombstone()) [[unlikely]] {
                        if (local_slot.is_volatile()) {
#ifdef BATCH_PERSIST
                            buckets[bidx].persist_line();
#else
                            slot.persist_and_clear();
#endif
                        }
                        result &= ~(1 << (idx - 1));
                        continue;
                    }
                    if (local_slot == nullptr || local_slot->key() != k) {
                        result &= ~(1 << (idx - 1));
                        // myLOG_DEBUG("idx = {}, result = {:x}\n", idx, result);
                        continue;
                    }

                    return &slot;
                }
#ifdef ZERO_BREAK
            if (empty) {
                int idx = __builtin_ffs(empty);
                auto &slot = buckets[bidx].slots[i + idx - 1];
                /* nullptr shows up */
                return nullptr;
            }
#endif
        }
        return nullptr;
    }

    std::pair<bool, bool> insert_from(kv_ptr<KV> *start, std::string_view k,
                                      size_t fingerprint, size_t bidx,
                                      kv_ptr<KV> &v) {
        // there is an implementation optimization that the insert can start from
        //      the first non-empty slot (provided in uniqueness check) without affecting correctness.
        //      because of an variant that the skipped slots can never be empty or hold the same
        //      key as the target key due to our designs.
        if (start == nullptr) { return {false, false}; }
        auto &bucket = buckets[bidx];

        const kv_ptr<KV> null{};
        // for (auto &slot : bucket.slots) {
        for (; start < bucket.slots.data() + KV_NUM_PER_BUCKET; ++start) {
            auto &slot = *start;
            if (slot == nullptr && slot.try_write(null, v)) {
                /* To tune the performance, this persist is optional */
                // fmt::print("[INFO] writed\n");
#ifdef CLEAR_IMMIDIATELY
                slot.clear_dirty_flag();
#endif
                return {true, false};
            } else if (slot != nullptr &&
                       slot.authenticate(fingerprint, slot) &&
                       slot->key() == k) {
                /* Found the same key (uniqueness check) */
                // fmt::print("[WARRNING] Same key\n");
                return {true, false};
            } else if (slot.is_copied()) {
                return {false, true};
            }
#ifdef DEBUG
            myLOG_DEBUG("insert {} checking {}({:04x})", k, slot->key(),
                        (int) slot.fingerprint);
#endif
        }
        return {false, false};
    }
    std::pair<bool, bool> load(kv_ptr<KV> *start, std::string_view k,
                               size_t fingerprint, size_t bidx, kv_ptr<KV> &v) {
        if (start == nullptr) { return {false, false}; }
        auto &bucket = buckets[bidx];

        const kv_ptr<KV> null{};
        // for (auto &slot : bucket.slots) {
        for (; start < bucket.slots.data() + KV_NUM_PER_BUCKET; ++start) {
            auto &slot = *start;
            if (slot == nullptr && slot.cas(0, v.data)) {
                /* To tune the performance, this persist is optional */
                // fmt::print("[INFO] writed\n");
#ifdef CLEAR_IMMIDIATELY
                slot.clear_dirty_flag();
#endif
                return {true, false};
            } else if (slot != nullptr &&
                       slot.authenticate(fingerprint, slot) &&
                       slot->key() == k) {
                /* Found the same key (uniqueness check) */
                // fmt::print("[WARRNING] Same key\n");
                return {true, false};
            } else if (slot.is_copied()) {
                return {false, true};
            }
#ifdef DEBUG
            myLOG_DEBUG("insert {} checking {}({:04x})", k, slot->key(),
                        (int) slot.fingerprint);
#endif
        }
        return {false, false};
    }
    bool insert(std::string_view k, size_t fingerprint, size_t bidx,
                kv_ptr<KV> &v) {
        auto &bucket = buckets[bidx];
        const kv_ptr<KV> null{};
        for (auto &slot : bucket.slots) {
            if (slot == nullptr && slot.try_write(null, v)) {
                /* To tune the performance, this persist is optional */
                // fmt::print("[INFO] writed\n");
#ifdef CLEAR_IMMIDIATELY
                slot.clear_dirty_flag();
#endif
                return true;
            } else if (slot != nullptr &&
                       slot.authenticate(fingerprint, slot) &&
                       slot->key() == k) {
                /* Found the same key (uniqueness check) */
                // fmt::print("[WARRNING] Same key\n");
                return true;
            }
#ifdef DEBUG
            myLOG_DEBUG("checking {}({:04x})", slot->key(),
                        (int) slot.fingerprint);
#endif
        }

        /* This bucket is full */
        return false;
    }
    int update(std::string_view k, size_t fingerprint, size_t bidx,
               kv_ptr<KV> &v) {
        auto &bucket = buckets[bidx];
        for (auto &slot : bucket.slots) {
            auto local_slot = slot;
            if (local_slot != nullptr &&
                local_slot.authenticate(fingerprint, slot) &&
                local_slot->key() == k) {
                bool ret = false;
                while (true) {
                    // local_slot = slot;
                    local_slot =
                            __atomic_load_n((size_t *) &slot, __ATOMIC_SEQ_CST);
                    if (local_slot.is_copied()) return RETRY;
                    if (local_slot.is_tombstone()) break;
                    ret = slot.try_write(local_slot, v);
                    // slot.cas(local_slot.data,
                    //          local_slot.data & ~kv_ptr<KV>::VOLATILE_FLAG_MASK);
                    if (ret) break;
                }
                if (ret) return SUCCESS;
                // slot = v;
                // return SUCCESS;
            }
        }
        return FAIL;
    }
    int avx_update(std::string_view k, size_t fingerprint,
                   size_t stale_fingerprint, size_t bidx, kv_ptr<KV> &v) {
        auto pslot = avx_find_slot(k, fingerprint, stale_fingerprint, bidx);
        if (pslot == nullptr) return FAIL;

        kv_ptr<KV> local_slot;

        bool ret = false;
        while (true) {
            local_slot = __atomic_load_n((size_t *) pslot, __ATOMIC_SEQ_CST);
            if (local_slot.is_copied()) [[unlikely]] { return RETRY; }
            if (local_slot.is_tombstone()) [[unlikely]] {
                if (pslot->is_volatile()) pslot->persist_and_clear();
                break;
            }
            ret = pslot->try_write(local_slot, v);
            // slot.cas(local_slot.data,
            //          local_slot.data & ~kv_ptr<KV>::VOLATILE_FLAG_MASK);
            if (ret) break;
        }
        if (ret) return SUCCESS;
        return FAIL;
    }
    int avx_delete(std::string_view k, size_t fingerprint,
                   size_t stale_fingerprint, size_t bidx, kv_ptr<KV> &v) {
        auto pslot = avx_find_slot(k, fingerprint, stale_fingerprint, bidx);
        if (pslot == nullptr) return FAIL;

        kv_ptr<KV> local_slot;

        bool ret = false;
        while (true) {
            local_slot = __atomic_load_n((size_t *) pslot, __ATOMIC_SEQ_CST);
            if (local_slot.is_copied()) [[unlikely]] { return RETRY; }
            if (local_slot.is_tombstone()) [[unlikely]] {
                if (pslot->is_volatile()) pslot->persist_and_clear();
                break;
            }
            ret = pslot->try_write(local_slot, v);
            // pslot->cas(local_slot.data,
            //            local_slot.data & ~kv_ptr<KV>::VOLATILE_FLAG_MASK);
            if (ret) break;
        }
        if (ret) return SUCCESS;

        return FAIL;
    }

    void refresh_fingerprints(size_t local_depth) {
        // #ifdef DEBUG
        // myLOG_DEBUG("update starts {}\n", (this - segment_ptr<KV>::base));
        // #endif
        size_t FP_align =
                64 - ((local_depth + BUCKET_INDEX_BIT_NUM) & (~7)) - 16;
        for (int bidx = 0; bidx < BUCKET_NUM_PER_SEGMENT; bidx++) {
            auto &bucket = buckets[bidx];
            for (auto &slot : bucket.slots) {
                if (auto t = slot; t != nullptr) {
                    if (t.stale) {
                        // auto hash = std::hash<decltype(t->key())>{}(t->key());
                        size_t hash = std::hash<std::string_view>{}(t->key());
#ifdef DEBUG
                        if (t->key() == std::string("399063506469976")) {
                            fmt::print("FP update {}, hash: {:016x}, "
                                       "({:04x})to({:04x}), fp align: {}\n",
                                       t->key(), hash, (int) t.fingerprint,
                                       (int) (hash >> FP_align) & 0xffffUL,
                                       FP_align);
                        }
#endif
                        if (!slot.cas(t.data,
                                      kv_ptr<KV>{t.offset, 0, 0,
                                                 (hash >> FP_align) & 0xffffUL}
                                              .data)) {
                            // myLOG_fatal("[UPDATE] cas failed\n");
                        }
                    }
                } else {
                    break;
                }
            }
            /* To tune the performance, persist the buckets line by line.*/
            pmem_persist(&bucket, sizeof(bucket));
            add_write_counter<KV>(sizeof(bucket));
        }
        // myLOG_DEBUG("update ends {}\n", (this - segment_ptr<KV>::base));
        // pmem_persist(this, sizeof(Segment));
    }
};


template<typename KV>
struct Directory {
    /* Data members */
    size_t depth;
    size_t capacity;
    std::atomic_bool resizing;
    c_ptr<segment_ptr<KV>> cur;
    c_ptr<segment_ptr<KV>> next;

    /* Constructors */
    Directory() = default;

    /* Interfaces */
    void initialize(size_t init_depth) {
        depth = init_depth;
        capacity = 1ul << init_depth;
        resizing = false;
        auto pool = pmem::obj::pool_by_vptr(this);
        fmt::print("alloc dir\n");
        /* Allocate directories */
        pmem::obj::transaction::run(pool, [&] {
            auto arr0 = pmem::obj::make_persistent<segment_ptr<KV>[]>(capacity);
            auto arr1 =
                    pmem::obj::make_persistent<segment_ptr<KV>[]>(capacity * 2);
            cur.offset = arr0.raw().off;
            next.offset = arr1.raw().off;
        });
        add_write_counter<KV>(sizeof(segment_ptr<KV>) * capacity * 3);

        fmt::print("alloc segments\n");
        /* Allocate segments and clear them */
        for (auto top = 0ul, bottom = 0ul, i = 0ul; i < capacity; ++i) {
            top = std::get<1>(Segment<KV>::allocator->alloc());
            if (i % 2 == 0) {
                bottom = std::get<1>(Segment<KV>::allocator->alloc());
            }
            cur[i] = segment_ptr<KV>{top, bottom, 0ul, 0ul};
            pmem_memset_persist(cur[i].get(0), 0, sizeof(Segment<KV>));
            add_write_counter<KV>(sizeof(Segment<KV>));

            if (i % 2 == 0) {
                pmem_memset_persist(cur[i].get(1), 0, sizeof(Segment<KV>));
                add_write_counter<KV>(sizeof(Segment<KV>));
            }
        }
        pmem_persist(cur.get(), sizeof(segment_ptr<KV>) * capacity);
        add_write_counter<KV>(sizeof(segment_ptr<KV>) * capacity);

        fmt::print("directory inited depth: {}\n", depth);
    }

    void upgrade_from(const Directory<KV> &d) {
        depth = d.depth + 1;
        capacity = 1 << depth;
        resizing = false;
        cur = d.next;
        auto pool = pmem::obj::pool_by_vptr(this);
        /* Allocate a new 'next' directory */
        pmem::obj::transaction::run(pool, [&] {
            auto arr1 =
                    pmem::obj::make_persistent<segment_ptr<KV>[]>(capacity * 2);
            next.offset = arr1.raw().off;
        });
        add_write_counter<KV>(sizeof(segment_ptr<KV>) * capacity * 2);
        fmt::print("directory doubleing from depth {} to {}\n", depth - 1,
                   depth);
    }

    segment_ptr<KV> &find_first(size_t sidx, const segment_ptr<KV> &sp) {
        size_t sidx_span = (1 << sp.diff);
        size_t sidx_base = sidx & (~(sidx_span - 1));
        return cur[sidx_base];
    }

    void flush_entries() {
        // time_guard tg_mv("Flush the current directory to next");
        size_t i = 0;
        while (i < capacity) {
            if (cur[i] != nullptr) {
                while (cur[i] != nullptr && !cur[i].lock()) {
                    // myLOG("Stuck {} of {}: {:x}\n", i, capacity, cur[i].data);
                }
                if (cur[i] == nullptr) {
                    ++i;
                    continue;
                } else {
                    // myLOG("cur[{} ~ {}] are processed (in {})\n", i, i + span - 1, capacity);
                    auto &first_sp = find_first(i, cur[i]);
                    if (&cur[i] - &first_sp != 0) {
                        myLOG("BUG here\n");
                        exit(0);
                    }

                    unsigned span = 1 << cur[i].diff;

                    for (size_t j = 0; j < span; ++j) {
                        segment_ptr<KV> next_seg(cur[i + j].offset0,
                                                 cur[i + j].offset1, 0,
                                                 cur[i + j].diff + 1);
                        next[(i + j) * 2].store(next_seg);
                        next[(i + j) * 2 + 1].store(next_seg);
                    }
                    pmem_persist(&next[i * 2],
                                 sizeof(segment_ptr<KV>) * span * 2);
                    for (size_t j = 0; j < span; ++j) { cur[i + j].clear(); }
                    i += span;
                }
            } else {
                ++i;
            }
        }
    }

    /* A latency bottleneck (now in background) */
    // void flush_entries() {
    //     // time_guard tg_mv("Flush the current directory to next");
    //     std::deque<size_t> blocked_idx;
    //     for (size_t i = 0; i < capacity; i++) {
    //         if (cur[i] != nullptr) {
    //             if (cur[i].is_locked() || !cur[i].lock()) [[unlikely]] {
    //                 // myLOG("cur[{} ~ {}] are inserted into blocked_list\n", i, i + span - 1);
    //                 unsigned span = 1 << cur[i].diff;
    //                 for (size_t j = 0; j < span; j++) {
    //                     blocked_idx.push_back(i + j);
    //                 }
    //                 i += span - 1;
    //                 continue;
    //             } else {
    //                 // myLOG("cur[{} ~ {}] are processed (in {})\n", i, i + span - 1, capacity);
    //                 unsigned span = 1 << cur[i].diff;
    //                 segment_ptr<KV> next_seg_p(cur[i].offset0, cur[i].offset1,
    //                                            0, cur[i].diff + 1);
    //                 /* For visibility */
    //                 __atomic_store(&next[i * 2].data, &next_seg_p.data,
    //                                __ATOMIC_RELAXED);
    //                 __atomic_store(&next[i * 2 + 1].data, &next_seg_p.data,
    //                                __ATOMIC_RELAXED);
    //                 for (size_t j = 1; j < span; j++) {
    //                     cur[i + j].lock();
    //                     segment_ptr<KV> next_seg_p(cur[i + j].offset0,
    //                                                cur[i + j].offset1, 0,
    //                                                cur[i + j].diff + 1);
    //                     __atomic_store(&next[(i + j) * 2].data,
    //                                    &next_seg_p.data, __ATOMIC_RELAXED);
    //                     __atomic_store(&next[(i + j) * 2 + 1].data,
    //                                    &next_seg_p.data, __ATOMIC_RELAXED);
    //                 }
    //                 pmem_persist(next.get() + i * 2,
    //                              2 * span * sizeof(next[i]));
    //                 add_write_counter<KV>(2 * span * sizeof(next[i]));
    //                 for (size_t j = 0; j < span; j++) {
    //                     cur[i + j].cas(cur[i + j], segment_ptr<KV>{0, 0, 0, 0});
    //                 }
    //                 i += span - 1;
    //             }
    //         }
    //     }
    //     /* Just for ensurance */
    //     while (!blocked_idx.empty()) {
    //         for (auto it = blocked_idx.begin(); it != blocked_idx.end();) {
    //             if (cur[*it] == nullptr) {
    //                 it += 1;
    //                 if (it != blocked_idx.end()) {
    //                     blocked_idx.erase(it - 1);
    //                 } else {
    //                     return;
    //                 }
    //             }
    //         }
    //     }
    // }
};

/* Do some maintainness */
template<typename KV>
struct BG_worker {
    /* Data members */
    inline static std::mutex mtx_segment, mtx_dir;
    /* TODO: change to persistent queues */
    inline static std::deque<std::pair<Segment<KV> *, size_t>> q_segment;
    inline static volatile bool dir_need_double = false;
    inline static c_ptr<Directory<KV>> old_dir;
    inline static std::atomic<bool> stop = 0;
    inline static std::thread *_worker;
    inline static steph<KV> *map = nullptr;

    /* Interfaces */
    void initialize(steph<KV> *in_map) {
        dir_need_double = false;
        stop = 0;
        map = in_map;
        /* TODO: set in which core? */
        _worker = new std::thread(work);
    }

    static void submit_refresh_request(Segment<KV> *target_segment,
                                       size_t local_depth) {
        std::lock_guard<std::mutex> lg(mtx_segment);
        q_segment.push_back({target_segment, local_depth});
    }

    static void submit_flush_dir_request(c_ptr<Directory<KV>> in_dir_ptr) {
        std::lock_guard<std::mutex> lg(mtx_dir);
        __atomic_store_n(&old_dir.offset, in_dir_ptr.offset, __ATOMIC_SEQ_CST);
        __atomic_store_n(&dir_need_double, true, __ATOMIC_SEQ_CST);
    }

    static void work() {
        using namespace std::chrono_literals;
        myLOG("Background worker starts working\n");
        size_t dir_depth = 0;
        pin_thread(119);
        size_t expected_stop = 1;
        while (true) {
            // if (stop.load()) { fmt::print("stop\n"); }
            // std::cout << "loop" << std::endl;
            if (dir_need_double) {
                // time_guard tg("[DOUBLE]");
                myLOG("background doubleing start\n");
                std::lock_guard<std::mutex> lg(mtx_dir);
                old_dir->flush_entries();
                pmem::obj::persistent_ptr<Directory<KV>> new_dir;
                auto pool = pmem::obj::pool_by_vptr(old_dir.get());
                pmem::obj::transaction::run(pool, [&] {
                    new_dir = pmem::obj::make_persistent<Directory<KV>>();
                });
                add_write_counter<KV>(sizeof(Directory<KV>));
                new_dir->upgrade_from(*old_dir);
                // map->dir.offset = new_dir.raw().off;
                __atomic_store_n(&map->dir.offset, new_dir.raw().off,
                                 __ATOMIC_SEQ_CST);

                pmem_persist(&map->dir, sizeof(c_ptr<Directory<KV>>));
                add_write_counter<KV>(sizeof(c_ptr<Directory<KV>>));

                dir_need_double = false;
                size_t directory_size_in_MB = sizeof(segment_ptr<KV>) *
                                              new_dir->capacity / (1ul << 20);
                myLOG("DOUBLE DIR towards {}, directory size is {} MB\n",
                      new_dir->depth, directory_size_in_MB);
            } else if (!q_segment.empty()) {
                // myLOG("q_segment is updating\n");
                // time_guard tg("[REFRESH FP]");
                // #ifdef PMHB_LATENCY
                //                 auto g = pmhb_ns::sample_guard<steph<KV>, pmhb_ns::REHASH>{};
                // #endif
                auto info = q_segment.front();
                // myLOG("updating FP with addr {} depth {}\n",
                //       (void *) std::get<0>(info), std::get<1>(info));
                std::get<0>(info)->refresh_fingerprints(std::get<1>(info));
                mtx_segment.lock();
                q_segment.pop_front();
                mtx_segment.unlock();
            } else if (stop.load()) {
                myLOG("Background worker finished\n");
                return;
            } else {
                std::this_thread::sleep_for(10us);
                // myLOG("sleeping\n");
                continue;
            }
        }
        return;
    }

    static void stop_work() {
        // bool ret = __sync_bool_compare_and_swap(&stop, 0, 1);

        stop.store(true);
        fmt::print("stored true to stop: {} \n", stop);

        _worker->join();
        fmt::print("Joined\n");
    }
};


template<typename KV>
struct kv_ptr {
    /* Types */
    using element_type = KV;
    using pointer = element_type *;
    using reference = element_type &;

    /* Data members */
    union {
        size_t data;
        struct {
            size_t offset : 45;
            size_t copied_flag : 1;
            size_t volatile_flag : 1;
            size_t stale : 1;
            size_t fingerprint : 16;
        };
    };
    inline static size_t pool_uuid_lo;
    // inline static uintptr_t pop;
    inline static constexpr size_t TOMB_STONE = (1ul << 45) - 1;
    inline static constexpr size_t COPIED_FLAG_MASK = 1ul << 45;
    inline static constexpr size_t VOLATILE_FLAG_MASK = 1ul << 46;

    /* Constructors */
    kv_ptr() = default;
    kv_ptr(kv_ptr const &) = default;
    kv_ptr(size_t _data) : data(_data) {}
    kv_ptr(nullptr_t) {}
    explicit kv_ptr(size_t _offset, size_t _volatile_flag, size_t _stale,
                    size_t _fingerprint)
        : offset(_offset), copied_flag(0), volatile_flag(_volatile_flag),
          stale(_stale), fingerprint(_fingerprint) {}

    /* Interfaces */
    bool cas(size_t const &expeceted, size_t const &desired) {
        return __sync_bool_compare_and_swap_8(&data, expeceted, desired);
    }

    bool authenticate(size_t _fingerprint, kv_ptr<KV> &original_slot) {
        if (is_tombstone()) [[unlikely]] {
            if (original_slot.is_volatile()) {
                original_slot.persist_and_clear();
            }
            return false;
        }
        auto mask = stale ? 0xfful : 0xfffful;
        _fingerprint = stale ? (_fingerprint >> 8) : _fingerprint;
        return (fingerprint & mask) == (_fingerprint & mask);
    }
    pointer get() const noexcept {
#ifdef GET_DEBUG
        if (offset >= 32ul << 30) [[unlikely]] {
            fmt::print("This get in kv_ptr is to access offset {}, data {:x}\n",
                       offset, data);
            exit(0);
        }
#endif
        return reinterpret_cast<pointer>(
                pmemobj_direct({pool_uuid_lo, offset}));
        // return reinterpret_cast<pointer>(pop + offset);
    }

    bool try_write(const kv_ptr &expected, const kv_ptr &desired) {
#ifdef NO_DIRTY_FLAG
        bool ret = cas((expected.data & ~COPIED_FLAG_MASK), desired.data);
#else
        bool ret = cas((expected.data & ~COPIED_FLAG_MASK),
                       desired.data | VOLATILE_FLAG_MASK);
#endif
        pmem_persist(this, sizeof(kv_ptr<KV>));
#if defined(COUNTING_WRITE)
        pmhb_ns::sample_guard<steph<KV>, pmhb_ns::WRITE_COUNT>(
                sizeof(kv_ptr<KV>));
#endif
        return ret;
    }
    bool is_volatile() {
#ifdef NO_DIRTY_FLAG
        return 0;
#else
        return volatile_flag;
#endif
    }
    bool is_tombstone() { return offset == TOMB_STONE; }
    bool is_copied() { return copied_flag; }
    kv_ptr set_copied() {
        kv_ptr expected;
        do {
            expected = *this;
        } while (!cas(expected.data, expected.data | COPIED_FLAG_MASK));
        expected.volatile_flag = 0;
        expected.copied_flag = 0;
        return expected;
    }

    /* PMWcas support. To tune the performance, the 'persist' is optional */
    void clear_dirty_flag() {
#ifndef NO_DIRTY_FLAG
        cas(this->data, this->data & ~VOLATILE_FLAG_MASK);
        // pmem_persist(this, 8);
#endif
    }

    void clear_for(const kv_ptr<KV> &snapshot) {
#ifndef NO_DIRTY_FLAG
        cas(snapshot.data, snapshot.data & ~VOLATILE_FLAG_MASK);
#endif
    }

    void persist_and_clear() {
#ifndef NO_DIRTY_FLAG
        pmem_persist(this, sizeof(kv_ptr<KV>));
        cas(this->data, this->data & ~VOLATILE_FLAG_MASK);
#endif
    }

    /* Operators */
    bool operator==(std::nullptr_t) const { return offset == 0; }
    bool operator!=(std::nullptr_t) const {
        return !(*this == nullptr);// NOLINT
    }
    pointer operator->() const noexcept { return get(); }
    reference operator*() const { return *get(); }

    /* For debug */
    std::string information() {
        return fmt::format("offset {:x}, FP {:04x}, stale {}, dirty bit {}, "
                           "rehash mark {}",
                           (uint64_t) offset, (uint32_t) fingerprint,
                           (uint32_t) stale, (uint32_t) volatile_flag,
                           (uint32_t) copied_flag);
    }
};


template<typename KV>
struct segment_ptr {
    /* Types */
    using element_type = Segment<KV>;
    using pointer = element_type *;

    /* Data members */
    union {
        size_t data{};
        struct {
            size_t offset0 : 30;
            size_t offset1 : 30;
            size_t lck : 1;
            size_t diff : 3;
        };
    };
    inline static pointer base = nullptr;

    /* Constructors */
    segment_ptr() = default;
    segment_ptr(segment_ptr const &) = default;
    explicit segment_ptr(size_t _offset0, size_t _offset1, size_t _lock,
                         size_t _diff)
        : offset0{_offset0}, offset1{_offset1}, lck{_lock}, diff{_diff} {}
    explicit segment_ptr(size_t _data) : data(_data) {}


    /* Interfaces */
    pointer get(size_t level) const noexcept {
#ifdef GET_DEBUG
        if (offset0 >= 1ul << 21 || offset1 >= 1ul << 21) [[unlikely]] {
            fmt::print("This get in segment_ptr is to access off0 {}, off1 {}, "
                       "data {:x}\n",
                       offset0, offset1, data);
            exit(0);
        }
#endif
        if (level == 0) {
            return base + offset0;
        } else if (level == 1) {
            return base + offset1;
        } else {
            /* Out of range */
            exit(1);
            return nullptr;
        }
    }

    bool cas(segment_ptr const &expeceted, segment_ptr const &desired) {
        return __sync_bool_compare_and_swap_8(&data, expeceted.data,
                                              desired.data);
    }

    void store(segment_ptr const &expeceted) {
        while (!cas(*this, expeceted))
            ;
        // __atomic_store(&data, &expeceted.data, __ATOMIC_RELAXED);
    }

    void clear() {
        while (*this != nullptr) {
            segment_ptr expeceted{*this}, zero{0, 0, 0, 0};
            cas(expeceted, zero);
        }
    }

    bool is_locked() const { return lck; }
    bool lock() {
        segment_ptr expeceted{*this}, locked{*this};
        locked.lck = 1;
        return !expeceted.lck && cas(expeceted, locked);
    }
    bool lock_for(segment_ptr<KV> sp) {
        segment_ptr expected, locked;
        expected = sp;
        expected.lck = 0;
        locked = expected;
        locked.lck = 1;
        return cas(expected, locked);
    }
    bool unlock() {
        segment_ptr expeceted{*this}, unlocked{*this};
        unlocked.lck = 0;
        return cas(expeceted, unlocked);
    }

    segment_ptr upgrade(unsigned off0) {
        return segment_ptr(off0, offset0, 0, diff ? diff - 1 : 0);
    }

    /* Operators */
    bool operator==(std::nullptr_t) const noexcept {
        return offset0 == 0 || offset1 == 0;
    }
    bool operator!=(std::nullptr_t) const noexcept {
        return !(*this == nullptr);// NOLINT
    }

    /* For debug */
    std::string information() {
        return fmt::format("s0 {} {}, s1 {} {}, lock {}, diff {}",
                           (uint32_t) offset0, (void *) get(0),
                           (uint32_t) offset1, (void *) get(1), (uint32_t) lck,
                           (uint32_t) diff);
    }
};

template<typename T>
struct c_ptr {
    /* Types */
    using element_type = T;
    using pointer = element_type *;
    using reference = element_type &;

    /* Data members */
    size_t offset{};
    inline static size_t pool_uuid_lo;

    /* Constructors */
    c_ptr() = default;
    c_ptr(c_ptr const &) = default;
    explicit c_ptr(size_t _offset) : offset(_offset) {}

    /* Interfaces */
    pointer get() const noexcept {
#ifdef GET_DEBUG
        if (offset >= 32ul << 30) [[unlikely]] {
            fmt::print("This get in c_ptr is to access offset {}, data {:x}\n",
                       offset, offset);
            exit(0);
        }
#endif
        return reinterpret_cast<pointer>(
                pmemobj_direct({pool_uuid_lo, offset}));
    }

    bool cas(c_ptr const &expeceted, c_ptr const &desired) {
        return __sync_bool_compare_and_swap_8(&offset, expeceted.offset,
                                              desired.offset);
    }

    /* Operators */
    pointer operator->() const noexcept { return get(); }
    reference operator*() const { return *get(); }
    reference operator[](size_t idx) const { return *(get() + idx); }
    element_type atomic_array_load(size_t idx) {
        return element_type(
                __atomic_load_8((size_t *) (get() + idx), __ATOMIC_SEQ_CST));
    }
    bool operator==(std::nullptr_t) const noexcept { return offset == 0; }
    bool operator!=(std::nullptr_t) const noexcept {
        return !(*this == nullptr);// NOLINT
    }
};


}// namespace steph_ns

#endif