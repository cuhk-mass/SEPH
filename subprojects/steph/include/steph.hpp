#ifndef STEPH_STEPH_HPP
#define STEPH_STEPH_HPP


/**
 * flags for performance breakdown 
 * 
 */
#if defined(BREAKDOWN_SOD)
// by default
#elif defined(BREAKDOWN_SO)
// semi lock-free + one-third split
#define ONE_THIRD_SPLIT_ONLY
#elif defined(BREAKDOWN_S)
// semi lock-free
#define TRADITIONAL_SPLIT
#elif defined(BREAKDOWN_BASE)
// baseline
#define TRADITIONAL_SPLIT
#define TRADITIONAL_LOCK
#endif


#ifdef TRADITIONAL_LOCK
#define NO_DIRTY_FLAG
#define TRADITIONAL_SPLIT
#endif


/**
 * flags for debugging or tunning, please ignore 
 */
// #define DEBUG
// #define BATCH_PERSIST
// #define NO_DIRTY_FLAG
// #define CLEAR_IMMIDIATELY
// #define NO_LOWER_ONLY_CHECK
// #define GET_DEBUG
// #define INSERT_DEBUG
// #define UPDATE_DEBUG
#define ZERO_BREAK

#include "alloc.hpp"
#include "config.hpp"
#include "substructure.hpp"

#include <filesystem>
#include <libpmem.h>
#include <libpmemobj.h>
#include <set>
#include <string_view>
#if defined PMHB_LATENCY || defined COUNTING_WRITE
#include "../../include/sample_guard.hpp"
#endif
using fmt::print;

namespace steph_ns {

template<typename KV>
struct steph {
    /* Data members */
    c_ptr<Directory<KV>> dir;
    inline static pmem::obj::pool<steph<KV>> pm_pool;

#ifndef SINGLE_THREAD
    inline static BG_worker<KV> hidden_worker;
#endif

    TOID_DECLARE(KV, 40);

    /* Constructors */
    steph() = delete;

    /* Interfaces */
    /* Create or open a stable hash table */
    static steph *open(std::filesystem::path pool_path,
                       size_t pool_size = DEFAULT_POOL_SIZE,
                       size_t init_depth = 8, size_t kv_uulo = 0) {
        steph *ret = nullptr;
        fmt::print("pool size is {}\n", pool_size);
        pool_size /= 2;// for the main pool and the segment pool.
        if (std::filesystem::exists(pool_path)) {
            fmt::print("open: To open the pool\n");
            pm_pool =
                    pmem::obj::pool<steph<KV>>::open(pool_path, SH_POOL_LAYOUT);
            ret = pm_pool.root().get();
            auto seg_path = std::filesystem::path{pool_path};
            seg_path += ".seg";
            print("seg path is {}\n", seg_path.c_str());
            Segment<KV>::allocator = stack_allocator<Segment<KV>>::open(
                    seg_path.c_str(), pool_size);
            if (kv_uulo) {
                kv_ptr<KV>::pool_uuid_lo = kv_uulo;
            } else {
                kv_ptr<KV>::pool_uuid_lo = pm_pool.root().raw().pool_uuid_lo;
            }
        } else {
            fmt::print("open: To create the pool\n");
            pm_pool = pmem::obj::pool<steph<KV>>::create(
                    pool_path, SH_POOL_LAYOUT, pool_size);
            {
                time_guard tg("Memset the main pool");
                /* To eliminate page fault for the main pool */
                pre_fault(pm_pool.handle(), pool_size);
            }
            ret = pm_pool.root().get();
            ret->initialize(pool_path, init_depth, pool_size, kv_uulo);
        }
        return ret;
    }

    /* Close the stable hash table */
    static void close(steph *map) {
#ifndef SINGLE_THREAD
        hidden_worker.stop_work();
#endif
        stack_allocator<Segment<KV>>::close(Segment<KV>::allocator);
        pm_pool.close();
    }

    /* Initialize the segment stack allocator */
    static void allocator_init(std::filesystem::path path, size_t pool_size) {
        auto seg_path = std::filesystem::path{path};
        seg_path += ".seg";
        print("seg path is {}", seg_path.c_str());
        Segment<KV>::allocator =
                stack_allocator<Segment<KV>>::open(seg_path.c_str(), pool_size);
        Segment<KV>::allocator->clear();
        segment_ptr<KV>::base = (Segment<KV> *) Segment<KV>::allocator;
        print("finished init\n");
    }

    /* Initialize the stable table with given depth */
    void initialize(std::filesystem::path kv_path, size_t init_depth,
                    size_t pool_size, size_t kv_uulo) {
        allocator_init(kv_path, pool_size);
        print("after allocator init\n");
        // auto pool = pmem::obj::pool_by_vptr(this);
        pmem::obj::persistent_ptr<Directory<KV>> d;
        pmem::obj::transaction::run(pm_pool, [&] {
            d = pmem::obj::make_persistent<Directory<KV>>();
        });
        add_write_counter<KV>(sizeof(Directory<KV>));

        c_ptr<Directory<KV>>::pool_uuid_lo = d.raw().pool_uuid_lo;
        c_ptr<Segment<KV>>::pool_uuid_lo = d.raw().pool_uuid_lo;
        c_ptr<segment_ptr<KV>>::pool_uuid_lo = d.raw().pool_uuid_lo;
        if (kv_uulo) {
            kv_ptr<KV>::pool_uuid_lo = kv_uulo;
        } else {
            kv_ptr<KV>::pool_uuid_lo = d.raw().pool_uuid_lo;
        }
        dir = c_ptr<Directory<KV>>{d.raw().off};

        print("dir allocated\n");
        dir->initialize(init_depth);
        print("dir init\n");
#ifndef SINGLE_THREAD
        hidden_worker.initialize(this);
#endif
        print("table inited depth: {}\n", init_depth);
    }


    /* Helper functions */
    inline static size_t segment_index(size_t hash, size_t global_depth) {
        return hash >> (64ul - global_depth);
    }

    inline static size_t bucket_index(size_t hash, size_t global_depth,
                                      size_t diff, size_t level) {
        return (hash >>
                (level + 64 - global_depth + diff - BUCKET_INDEX_BIT_NUM)) &
               (BUCKET_NUM_PER_SEGMENT - 1ul);
    }

    inline static size_t fingerprint(size_t hash, size_t depth, size_t diff) {
#if !defined(ONE_THIRD_SPLIT_ONLY) && !defined(TRADITIONAL_SPLIT)
        auto bit_used = depth - diff + BUCKET_INDEX_BIT_NUM;
        auto alignment = bit_used & (~(FINGERPRINT_BIT_ALIGNMENT - 1ul));
        return (hash << (alignment)) >> 48ul;
#else
        auto mask = (1ul << 16) - 1;
        return hash & mask;
#endif
    }

    inline static size_t stale_fingerprint(size_t hash, size_t depth,
                                           size_t diff) {
#if !defined(ONE_THIRD_SPLIT_ONLY) && !defined(TRADITIONAL_SPLIT)
        auto bit_used = depth - diff + BUCKET_INDEX_BIT_NUM;
        bit_used = bit_used >= 8 ? bit_used - 8 : bit_used;
        auto alignment = bit_used & (~(FINGERPRINT_BIT_ALIGNMENT - 1ul));
        return (hash << (alignment)) >> 48ul;
#else
        auto mask = (1ul << 16) - 1;
        return hash & mask;
#endif
    }

    /* Interfaces */
    KV *search(std::string_view k) {
#ifdef PMHB_LATENCY
        /* To test the latency of this search operation */
        auto g = pmhb_ns::sample_guard<steph<KV>, pmhb_ns::SEARCH>{};
#endif

#ifdef UPDATE_DEBUG
        time_guard tg("S: ");
#endif
        size_t hash = std::hash<std::string_view>{}(k);
    search_retry:
        const auto &d = *(dir.get());
        auto depth = d.depth;
        bool resizing = false;
        /* An temp segment_ptr for atomic operations */
        auto sp = d.cur[segment_index(hash, depth)];
        if (sp == nullptr) {
            resizing = true;
            depth += 1;
            sp = d.next[segment_index(hash, depth)];
            if (sp == nullptr) { exit(0); }
        }
        // if (sp.is_locked()) { continue; }

        /* Two physical segments in a logical segment has different depth, so they has different indexing information */
        size_t fp[2] = {fingerprint(hash, depth, sp.diff),
                        fingerprint(hash, depth - 1, sp.diff)};
        size_t bidx[2] = {bucket_index(hash, depth, sp.diff, 0),
                          bucket_index(hash, depth, sp.diff, 1)};
        /* To tune the performance, try or not to prefetch the two XPline */
        // __builtin_prefetch(&sp.get(0)->buckets[bidx[0]], 0, 1);
        // __builtin_prefetch(&sp.get(1)->buckets[bidx[1]], 0, 1);

#ifdef DEBUG
        print("[Search] {}({:016x} FFP {:04x} SFP {:04x}) depth {} in "
              "{}[{}({})].buckets[{} & {}], "
              "with s_ptr "
              "{}\n",
              k, hash, fp[0], fp[1], depth, resizing ? "next" : "cur",
              segment_index(hash, depth), (int) sp.diff, bidx[0], bidx[1],
              sp.information());
#endif
        KV *ret;
        kv_ptr<KV> *first_empty[2];
        bool retry = false;
        for (auto &level : {1, 0}) {

            // we use avx to boost the searching as a code optimization which does not affect
            //      the correctness because:
            //  there is an invariant in our design: one slot can only store one key for its
            //      whole life (since only one key is inserted and it is not available to
            //      other key after it is deleted.)


            std::tie(ret, first_empty[level], retry) =
                    sp.get(level)->avx_search(
                            k, fp[level],
                            stale_fingerprint(hash, depth, sp.diff),
                            bidx[level]);

            if (retry) { goto search_retry; }
            if (ret != nullptr) { return ret; }
        }
        return nullptr;
    }

    bool insert(std::string_view k, std::string_view v, kv_ptr<KV> pkv = {},
                bool is_load = false) {
#ifdef PMHB_LATENCY
        /* To test the latency of this insert operation */
        auto g = pmhb_ns::sample_guard<steph<KV>, pmhb_ns::INSERT>{};
#endif
#ifdef INSERT_DEBUG
        time_guard tg("I: ");
#endif
        size_t hash = std::hash<std::string_view>{}(k);

        do {
#ifdef INSERT_DEBUG
            time_guard tg_do("do ", tg);
#endif
            c_ptr<Directory<KV>> d = dir;
            size_t depth = d->depth;
            bool resizing = false;
            segment_ptr<KV> sp =
                    d->cur.atomic_array_load(segment_index(hash, depth));
            //     segment_ptr<KV> sp = d->cur[segment_index(hash, depth)];
            if (sp == nullptr) {
                resizing = true;
                depth += 1;
                sp = d->next.atomic_array_load(segment_index(hash, depth));
            }

            size_t fp[2] = {fingerprint(hash, depth, sp.diff),
                            fingerprint(hash, depth - 1, sp.diff)};
            size_t bidx[2] = {bucket_index(hash, depth, sp.diff, 0),
                              bucket_index(hash, depth, sp.diff, 1)};
            __builtin_prefetch(&sp.get(0)->buckets[bidx[0]], 1, 1);
            __builtin_prefetch(&sp.get(1)->buckets[bidx[1]], 1, 1);

#ifdef TRADITIONAL_LOCK
            /* Lock all bucket */
            if (sp.get(0)->buckets[bidx[0]].lock() == false) { continue; }

            if (sp.get(1)->buckets[bidx[1]].lock() == false) {
                sp.get(0)->buckets[bidx[0]].unlock();
                continue;
            }
#endif

            bool retry = false;

            /* Uniqueness check */
            /* To tune the performance, we can only search one bucket for uniqueness check */
            kv_ptr<KV> *first_empty[2] = {nullptr, nullptr};
#ifdef NO_LOWER_ONLY_CHECK
            for (auto const &level : {0, 1}) {
#else
            for (auto const &level : {1, 0}) {
#endif
                KV *ret;
#ifdef INSERT_DEBUG
                time_guard tg1("uniq", tg);
#endif
#ifdef DEBUG
                fmt::print(
                        "\n[SEARCH for uniq] key {}, hash {:016x}, l {}, depth "
                        "{}, diff {}, sidx {}, bidx {}, b {:b}\n",
                        k, hash, level, depth, (unsigned) sp.diff,
                        segment_index(hash, depth), bidx[level], bidx[level]);
#endif
                std::tie(ret, first_empty[level], retry) =
                        sp.get(level)->avx_search(
                                k, fp[level],
                                stale_fingerprint(hash, depth, sp.diff),
                                bidx[level], 1);

                if (retry) { break; }
                if (ret) {
                    // fmt::print("Fail the uniqueness in the bottom bucket\n");
#ifdef WRITE_KV
                    pmemobj_free(&oid);
#endif
#ifdef TRADITIONAL_LOCK
                    sp.get(0)->buckets[bidx[0]].unlock();
                    sp.get(1)->buckets[bidx[1]].unlock();
#endif
                    return false;
                }
            }
            if (retry) {
#ifdef TRADITIONAL_LOCK
                sp.get(0)->buckets[bidx[0]].unlock();
                sp.get(1)->buckets[bidx[1]].unlock();
#endif
                continue;
            }
            /* Try to insert */
            bool ret = false;
            for (auto const &level : std::array{1, 0}) {
#ifdef INSERT_DEBUG
                time_guard tg1("write", tg);
#endif
                pkv.fingerprint = fp[level];
#ifdef DEBUG
                myLOG("\n[INSERT] key:{} (hash:{:016x}), on level: {}, L:{}, "
                      "diff:"
                      "{}, "
                      "sidx: "
                      "{}, bidx: {}, PS_offset: {}\n",
                      k, hash, level, depth, (unsigned) sp.diff,
                      segment_index(hash, depth), bidx[level],
                      level ? (int) sp.offset1 : (int) sp.offset0);
#endif
                if (is_load == false) {
                    // during the run phase (including load in ycsb load test),
                    //      we insert with dirty flag set.
                    std::tie(ret, retry) = sp.get(level)->insert_from(
                            first_empty[level], k, fp[level], bidx[level], pkv);
                } else {
                    // during the load phase, we insert without set dirty flag, becaseu
                    //      we assume there is a interval between load phase and run phase
                    //      and the interval is enough for persisting the hash table so that
                    //      the dirty bit is not necessary.
                    std::tie(ret, retry) = sp.get(level)->load(
                            first_empty[level], k, fp[level], bidx[level], pkv);
                }
                // ret = sp.get(level)->insert(k, fp[level], bidx[level], pkv);
                if (retry) { break; }
                if (ret) {
#ifdef TRADITIONAL_LOCK
                    sp.get(0)->buckets[bidx[0]].unlock();
                    sp.get(1)->buckets[bidx[1]].unlock();
#endif
#ifdef DEBUG
                    myLOG("\n[INSERTED] key:{} (hash:{:016x}), on level: {}, "
                          "L:{}, "
                          "diff:"
                          "{}, "
                          "sidx: "
                          "{}, bidx: {}, PS_offset: {}\n",
                          k, hash, level, depth, (unsigned) sp.diff,
                          segment_index(hash, depth), bidx[level],
                          level ? (int) sp.offset1 : (int) sp.offset0);
#endif
                    return ret;
                }
            }
            // fmt::print("InsertFailed\n");
#ifdef TRADITIONAL_LOCK
            sp.get(0)->buckets[bidx[0]].unlock();
            sp.get(1)->buckets[bidx[1]].unlock();
#endif
            /* All candidate buckets are full */
            if (retry || (d.offset != dir.offset)) {
                /* Splitting or directory doubling */
#ifdef INSERT_DEBUG
                if (!retry) { time_guard tg2("retry(dir changed)", tg); }
#endif
                continue;
            }
            if (resizing && sp.diff == 0) {
                /* The segment is ahead of the directory doubling, and needs to wait */
                // exit(0);
                continue;
            }


            size_t sidx_span = (1 << sp.diff);
            size_t sidx_base = segment_index(hash, depth) & (~(sidx_span - 1));
            auto d_in_use = resizing ? d->next : d->cur;
            if (d_in_use[sidx_base] != nullptr &&
                d_in_use[sidx_base].lock_for(sp)) {
                /* Lock the first segment_ptr and start the split process */
                // myLOG_DEBUG("split {}~{}\n", sidx_base, sidx_base + sidx_span - 1);
#ifdef INSERT_DEBUG
                time_guard tg("non-trival-insert-work");
#endif

                segment_ptr<KV> new_segment0, new_segment1;
                for (size_t i = 1; i < sidx_span; i++) {
                    /* lock the other segments */
                    d_in_use[sidx_base + i].lock();
                }
#if defined(ONE_THIRD_SPLIT_ONLY)
                std::tie(new_segment0, new_segment1) =
                        lazy_split_onethird_spit_only(
                                sp, depth,
                                bidx[1] & (BUCKET_NUM_PER_SEGMENT >> 1));
#elif defined(TRADITIONAL_SPLIT)
                std::tie(new_segment0, new_segment1) = traditional_split(
                        sp, depth, bidx[1] & (BUCKET_NUM_PER_SEGMENT >> 1));
#else
            std::tie(new_segment0, new_segment1) = lazy_split(
                    sp, depth, bidx[1] & (BUCKET_NUM_PER_SEGMENT >> 1));
#endif
                if (sp.diff == 0) {
                    /* Doubling the directory */

#ifdef INSERT_DEBUG
                    time_guard tg0("non-trival-rehash-branch");
#endif
                    bool expected = false;
#ifdef PMHB_LATENCY
                    auto g =
                            pmhb_ns::sample_guard<steph<KV>, pmhb_ns::DOUBLE>{};
#endif
                    if (std::atomic_compare_exchange_strong(&d->resizing,
                                                            &expected, true)) {
                        /* Directory Doubling */
#ifndef SINGLE_THREAD
                        hidden_worker.submit_flush_dir_request(d);
#else
                        for (size_t i = 0; i < d->capacity; i++) {
                            d->next[2 * i] = d->next[2 * i + 1] = d->cur[i];
                            d->next[2 * i].diff += 1;
                            d->next[2 * i + 1].diff += 1;
                        }
                        pmem_persist(d->next.get(),
                                     d->capacity * 2 * sizeof(segment_ptr<KV>));
                        add_write_counter<KV>(d->capacity * 2 *
                                              sizeof(segment_ptr<KV>));
                        pmem::obj::persistent_ptr<Directory<KV>> new_dir;
                        auto pool = pmem::obj::pool_by_vptr(this);
                        pmem::obj::transaction::run(pool, [&] {
                            new_dir =
                                    pmem::obj::make_persistent<Directory<KV>>();
                        });
                        add_write_counter<KV>(sizeof(Directory<KV>));
                        new_dir->upgrade_from(d);
                        dir.offset = new_dir.raw().off;
                        myLOG("DOUBLE DIR towards {}\n", d->depth + 1);
#endif
                    }
                    d->next[sidx_base * 2].store(new_segment0);
                    d->next[sidx_base * 2 + 1].store(new_segment1);
                    pmem_persist(&d->next[sidx_base * 2],
                                 2 * sizeof(segment_ptr<KV>));
                    d->cur[sidx_base].clear();
                    pmem_persist(&d->cur[sidx_base], sizeof(segment_ptr<KV>));
                    add_write_counter<KV>(3 * sizeof(segment_ptr<KV>));
                } else {
                    /* Normal split */

#ifdef INSERT_DEBUG
                    time_guard tg0("non-trival-normal-branch");
#endif

                    /* persist the segment pointer before it is unlocked */
                    new_segment0.lck = 1;
                    new_segment1.lck = 1;
                    for (size_t i = 0; i < sidx_span; i++) {
                        d_in_use[sidx_base + i].store(i < sidx_span / 2
                                                              ? new_segment0
                                                              : new_segment1);
                    }
                    pmem_persist(&d_in_use[sidx_base],
                                 sidx_span * sizeof(segment_ptr<KV>));
                    for (size_t i = 0; i < sidx_span; i++) {
                        d_in_use[sidx_base + i].unlock();
                    }
                    pmem_persist(&d_in_use[sidx_base],
                                 sidx_span * sizeof(segment_ptr<KV>));
                    add_write_counter<KV>(sidx_span * sizeof(segment_ptr<KV>));
                }
            }
        } while (true);

        return false;
    }

    bool update(std::string_view k, std::string_view v, kv_ptr<KV> pkv) {
#ifdef PMHB_LATENCY
        /* To test the latency of this insert operation */
        auto g = pmhb_ns::sample_guard<steph<KV>, pmhb_ns::UPDATE>{};
#endif
#ifdef UPDATE_DEBUG
        time_guard tg("U: ");
#endif
        size_t hash = std::hash<std::string_view>{}(k);

    update_retry:
#ifdef UPDATE_DEBUG
        time_guard tg_do("do ", tg);
#endif
        auto d = dir;
        auto depth = d->depth;
        bool resizing = false;
        auto sp = d->cur[segment_index(hash, depth)];
        if (sp == nullptr) {
            resizing = true;
            depth += 1;
            sp = d->next[segment_index(hash, depth)];
        }

        // print("Start to insert {} to sp ({})\n", k, sp.information());

        size_t fp[2] = {fingerprint(hash, depth, sp.diff),
                        fingerprint(hash, depth - 1, sp.diff)};
        size_t bidx[2] = {bucket_index(hash, depth, sp.diff, 0),
                          bucket_index(hash, depth, sp.diff, 1)};

        /* Uniqueness check */
        /* To tune the performance, we can only search one bucket for uniqueness check */
        int ret = false;

        /* Try to update */
        for (auto const &level : std::array{1, 0}) {
#ifdef UPDATE_DEBUG
            time_guard tg1("write", tg);
#endif
            pkv.fingerprint = fp[level];
#ifdef DEBUG
            myLOG("\n[INSERT] key {}, hash {:016x}, l {}, depth {}, diff "
                  "{}, "
                  "sidx "
                  "{}, bidx {}, {}, in sa {}\n",
                  k, hash, level, depth, (unsigned) sp.diff,
                  segment_index(hash, depth), bidx[level], pkv.data,
                  (void *) sp.get(level));
#endif

            ret = sp.get(level)->avx_update(
                    k, fp[level], stale_fingerprint(hash, depth, sp.diff),
                    bidx[level], pkv);
            // ret = sp.get(level)->update(k, fp[level], bidx[level], pkv);
            // on encountering duplicate key, return a pointer to it
            if (ret == SUCCESS) { return true; }
            if (ret == RETRY) { goto update_retry; }
        }

        return false;
    }

    bool Delete(std::string_view k) {
#ifdef PMHB_LATENCY
        /* To test the latency of this insert operation */
        auto g = pmhb_ns::sample_guard<steph<KV>, pmhb_ns::DELETE>{};
#endif
#ifdef DELETE_DEBUG
        time_guard tg("U: ");
#endif
        size_t hash = std::hash<std::string_view>{}(k);

    delete_retry:
#ifdef DELETE_DEBUG
        time_guard tg_do("do ", tg);
#endif
        auto d = dir;
        auto depth = d->depth;
        bool resizing = false;
        auto sp = d->cur[segment_index(hash, depth)];
        if (sp == nullptr) {
            resizing = true;
            depth += 1;
            sp = d->next[segment_index(hash, depth)];
        }

        // print("Start to insert {} to sp ({})\n", k, sp.information());

        size_t fp[2] = {fingerprint(hash, depth, sp.diff),
                        fingerprint(hash, depth - 1, sp.diff)};
        size_t bidx[2] = {bucket_index(hash, depth, sp.diff, 0),
                          bucket_index(hash, depth, sp.diff, 1)};

        /* Uniqueness check */
        /* To tune the performance, we can only search one bucket for uniqueness check */
        int ret = false;

        kv_ptr<KV> pkv(kv_ptr<KV>::TOMB_STONE, 0, 0, 0);

        /* Try to delete */
        for (auto const &level : std::array{1, 0}) {
#ifdef DELETE_DEBUG
            time_guard tg1("write", tg);
#endif
            pkv.fingerprint = fp[level];
#ifdef DEBUG
            myLOG("\n[INSERT] key {}, hash {:016x}, l {}, depth {}, diff "
                  "{}, "
                  "sidx "
                  "{}, bidx {}, {}, in sa {}\n",
                  k, hash, level, depth, (unsigned) sp.diff,
                  segment_index(hash, depth), bidx[level], pkv.data,
                  (void *) sp.get(level));
#endif

            ret = sp.get(level)->avx_delete(
                    k, fp[level], stale_fingerprint(hash, depth, sp.diff),
                    bidx[level], pkv);
            // ret = sp.get(level)->update(k, fp[level], bidx[level], pkv);
            // on encountering duplicate key, return a pointer to it
            if (ret == SUCCESS) { return true; }
            if (ret == RETRY) { goto delete_retry; }
        }
        return false;
    }
    auto lazy_split(segment_ptr<KV> to_split, size_t depth, size_t base) {

#ifdef DEBUG
        fmt::print("[SPLIT] split segment (info {}), depth = {}, "
                   "base={}\n",
                   to_split.information(), depth, base);
#endif

#ifdef PMHB_LATENCY
        auto g = pmhb_ns::sample_guard<steph<KV>, pmhb_ns::REHASH>{};
#endif

#ifdef SPLIT_DEBUG
        time_guard split_guard("S: ");
#endif

        /* the bottom layer is one less deep than the top */
        depth--;
        auto src_segment = to_split.get(1);
        const unsigned bit_used =
                (depth - to_split.diff + BUCKET_INDEX_BIT_NUM) & 7;
        /* "bit_used>=6" equal to "((bit_used + 2) % 8) < (bit_used % 8)" */
        const bool need_update = bit_used >= 6;
        const unsigned fp_shift_bits = 16 - (bit_used + 2);
        size_t FP_align = 64 - ((depth + 2 + BUCKET_INDEX_BIT_NUM) & (~7)) - 16;
        size_t involved_kv = 0;

        /* To tune the performance, use local segment or PM segment directly? */
        // Segment<KV> dst_in_cache[2];
        // memset(&dst_in_cache[0], 0, sizeof(Segment<KV>));
        // memset(&dst_in_cache[1], 0, sizeof(Segment<KV>));
        std::array<Bucket<KV>, 4> dst_in_cache;


        auto [addr0, off0] = Segment<KV>::allocator->alloc();
        auto [addr1, off1] = Segment<KV>::allocator->alloc();
        Segment<KV> *dst[2] = {addr0, addr1};
        {
#ifdef SPLIT_DEBUG
            time_guard split_guard_copy("copy", split_guard);
#endif
            /* Copy slots from bottom level to new segments */
            for (size_t i = 0; i < BUCKET_NUM_PER_SEGMENT / 2; i++) {
                memset(&dst_in_cache, 0, sizeof(dst_in_cache));
                unsigned dst_bidx_base =
                        (i << 2) & (BUCKET_NUM_PER_SEGMENT - 1);
                unsigned dst_sidx =
                        (unsigned) (i >= BUCKET_NUM_PER_SEGMENT / 4);
                unsigned slot_cnt[4] = {0, 0, 0, 0};

                for (size_t j = 0; j < KV_NUM_PER_BUCKET; j++) {
                    unsigned shunt;
                    kv_ptr<KV> &slot = src_segment->buckets[base + i].slots[j];

                    //                     if (slot == nullptr) {
                    // #ifdef ZERO_BREAK
                    //                         break;
                    // #else
                    //                         continue;
                    // #endif
                    //                     }
                    kv_ptr<KV> copied_slot = slot.set_copied();

                    if (copied_slot == nullptr) { continue; }
                    // if (slot.is_tombstone() && slot.is_volatile()) {
                    //     slot.persist_and_clear();
                    //     continue;
                    // }
                    involved_kv++;
                    if (slot.stale && need_update) [[unlikely]] {
                        /* The 16-bit fingerprint runs out, calculate the fingerprint for shunt */
                        std::string_view k = slot->key();
                        size_t hash_key = std::hash<std::string_view>{}(k);
                        shunt = (hash_key >> (64 - depth + to_split.diff -
                                              BUCKET_INDEX_BIT_NUM - 2)) &
                                3;
                        copied_slot.stale = 0;
                        copied_slot.fingerprint =
                                (hash_key >> FP_align) & 0xffffUL;
                        // copied_slot =
                        //         kv_ptr<KV>{slot.offset, 0, 0,
                        //                    (hash_key >> FP_align) & 0xffffUL};
                        // fmt::print("split unlikely\n");

                    } else {
                        shunt = ((copied_slot.fingerprint >>
                                  (copied_slot.stale ? fp_shift_bits - 8
                                                     : fp_shift_bits)) &
                                 3);
                        if (need_update) { copied_slot.stale = 1; }
                    }
#ifdef DEBUG
                    fmt::print("[SPLIT] key {}, to seg: {}, from bucket {} to "
                               "{}, slot fp: {:04x}, slot "
                               "cp_flag: {}, CP_FP: {:04x}, CP_flag: {}\n",
                               slot->key(), dst_sidx ? off1 : off0, base + i,
                               dst_bidx_base + shunt, (int) slot.fingerprint,
                               (int) slot.copied_flag,
                               (int) copied_slot.fingerprint,
                               (int) copied_slot.copied_flag);
#endif
                    dst_in_cache[shunt].slots[slot_cnt[shunt]++] = copied_slot;
                }

                pmem_memcpy_persist(&dst[dst_sidx]->buckets[dst_bidx_base],
                                    &dst_in_cache, sizeof(dst_in_cache));
                pmem_persist(&src_segment->buckets[base + i],
                             sizeof(src_segment->buckets[base + i]));
            }
        }
#ifdef PMHB_LATENCY
        pmhb_ns::sample_guard<steph<KV>, pmhb_ns::RESIZE_ITEM_NUMBER>{
                involved_kv};
#endif

#ifdef SPLIT_DEBUG
        time_guard split_guard_behind_half("F_alc", split_guard);
#endif


        {
#ifdef SPLIT_DEBUG
            time_guard split_guard_memcpy_persisit("toPM", split_guard);
#endif
            // pmem_memcpy_persist(addr0, &dst_in_cache[0], sizeof(Segment<KV>));
            // pmem_memcpy_persist(addr1, &dst_in_cache[1], sizeof(Segment<KV>));
        }
        if (need_update) {
#ifndef SINGLE_THREAD
            hidden_worker.submit_refresh_request(addr0,
                                                 depth - to_split.diff + 2);
            hidden_worker.submit_refresh_request(addr1,
                                                 depth - to_split.diff + 2);
#else
            addr0->refresh_fingerprints(depth - to_split.diff + 2);
            addr1->refresh_fingerprints(depth - to_split.diff + 2);
#endif
        }

        return std::tuple{to_split.upgrade(off0), to_split.upgrade(off1)};
    }


    auto lazy_split_onethird_spit_only(segment_ptr<KV> to_split, size_t depth,
                                       size_t base) {
        /* without foreseers */
#ifdef DEBUG
        fmt::print("[SPLIT] split segment (info {}), depth = {}, "
                   "base={}\n",
                   to_split.information(), depth, base);
#endif

#ifdef PMHB_LATENCY
        auto g = pmhb_ns::sample_guard<steph<KV>, pmhb_ns::REHASH>{};
#endif
        /* the bottom layer is one less deep than the top */
        depth--;
        auto src_segment = to_split.get(1);
        const unsigned bit_used =
                (depth - to_split.diff + BUCKET_INDEX_BIT_NUM) & 7;
        /* "bit_used>=6" equal to "((bit_used + 2) % 8) < (bit_used % 8)" */
        const bool need_update = bit_used >= 6;
        const unsigned fp_shift_bits = 16 - (bit_used + 2);
        size_t FP_align = 64 - ((depth + 2 + BUCKET_INDEX_BIT_NUM) & (~7)) - 16;
        size_t involved_kv = 0;

        /* To tune the performance, use local segment or PM segment directly? */
        // Segment<KV> dst_in_cache[2];
        // memset(&dst_in_cache[0], 0, sizeof(Segment<KV>));
        // memset(&dst_in_cache[1], 0, sizeof(Segment<KV>));
        std::array<Bucket<KV>, 4> dst_in_cache;


        auto [addr0, off0] = Segment<KV>::allocator->alloc();
        auto [addr1, off1] = Segment<KV>::allocator->alloc();
        Segment<KV> *dst[2] = {addr0, addr1};
        /* Copy slots from bottom level to new segments */
        for (size_t i = 0; i < BUCKET_NUM_PER_SEGMENT / 2; i++) {
            memset(&dst_in_cache, 0, sizeof(dst_in_cache));
            unsigned dst_bidx_base = (i << 2) & (BUCKET_NUM_PER_SEGMENT - 1);
            unsigned dst_sidx = (unsigned) (i >= BUCKET_NUM_PER_SEGMENT / 4);
            unsigned slot_cnt[4] = {0, 0, 0, 0};

            for (size_t j = 0; j < KV_NUM_PER_BUCKET; j++) {
                unsigned shunt;
                kv_ptr<KV> copied_slot;
                kv_ptr<KV> &slot = src_segment->buckets[base + i].slots[j];

                slot.set_copied();
                if (slot == nullptr) { continue; }
                if (slot.is_tombstone() && slot.is_volatile()) {
                    slot.persist_and_clear();
                    continue;
                }
                involved_kv++;
                /* The 16-bit fingerprint runs out, calculate the fingerprint for shunt */
                std::string_view k = slot->key();
                size_t hash_key = std::hash<std::string_view>{}(k);
                shunt = (hash_key >> (64 - depth + to_split.diff -
                                      BUCKET_INDEX_BIT_NUM - 2)) &
                        3;
                copied_slot = kv_ptr<KV>{slot.offset, 0, 0, slot.fingerprint};
                dst_in_cache[shunt].slots[slot_cnt[shunt]++] = copied_slot;
            }

            pmem_memcpy_persist(&dst[dst_sidx]->buckets[dst_bidx_base],
                                &dst_in_cache, sizeof(dst_in_cache));
            pmem_persist(&src_segment->buckets[base + i],
                         sizeof(src_segment->buckets[base + i]));
        }
#ifdef PMHB_LATENCY
        pmhb_ns::sample_guard<steph<KV>, pmhb_ns::RESIZE_ITEM_NUMBER>{
                involved_kv};
#endif
        return std::tuple{to_split.upgrade(off0), to_split.upgrade(off1)};
    }

    auto traditional_split(segment_ptr<KV> to_split, size_t depth,
                           size_t base) {
        /* without foreseers */
#ifdef DEBUG
        fmt::print("[SPLIT] split segment (info {}), depth = {}, "
                   "base={}\n",
                   to_split.information(), depth, base);
#endif

#ifdef PMHB_LATENCY
        auto g = pmhb_ns::sample_guard<steph<KV>, pmhb_ns::REHASH>{};
#endif
        /* the bottom layer is one less deep than the top */
        // depth--;
        // auto src_segment = to_split.get(0);
        // const unsigned bit_used =
        //         (depth - to_split.diff + BUCKET_INDEX_BIT_NUM) & 7;
        // /* "bit_used>=6" equal to "((bit_used + 2) % 8) < (bit_used % 8)" */
        // const bool need_update = bit_used >= 6;
        // const unsigned fp_shift_bits = 16 - (bit_used + 2);
        // size_t FP_align =
        //         64 - ((depth + 2 + BUCKET_INDEX_BIT_NUM) & (~7)) - 16;
        size_t involved_kv = 0;

#ifdef TRADITIONAL_LOCK
        /* lock all buckets */
        Segment<KV> *sg;
        sg = to_split.get(0);
        for (size_t i = 0; i < BUCKET_NUM_PER_SEGMENT; i++) {
            while (sg->buckets[i].lock() == false)
                ;
        }
        sg = to_split.get(1);
        for (size_t i = base; i < base + BUCKET_NUM_PER_SEGMENT / 2; i++) {
            while (sg->buckets[i].lock() == false)
                ;
        }
#endif
        std::array<Bucket<KV>, 2> dst_in_cache;

        /* To tune the performance, use local segment or PM segment directly? */
        auto [addr0, off0] = Segment<KV>::allocator->alloc();
        auto [addr1, off1] = Segment<KV>::allocator->alloc();
        auto [addr2, off2] = Segment<KV>::allocator->alloc();
        Segment<KV> *dst[3] = {addr0, addr1, addr2};


        // depth = depth;
        // copy to the top level
        Segment<KV> *src_segment = to_split.get(0);
        for (size_t i = 0; i < BUCKET_NUM_PER_SEGMENT; i++) {
            memset(&dst_in_cache, 0, sizeof(dst_in_cache));
            unsigned slot_cnt[2] = {0, 0};

            for (size_t j = 0; j < KV_NUM_PER_BUCKET; j++) {
                unsigned shunt;
                kv_ptr<KV> copied_slot;
                kv_ptr<KV> &slot = src_segment->buckets[i].slots[j];
#ifndef TRADITIONAL_LOCK
                slot.set_copied();
#endif
                if (slot == nullptr) { continue; }
#ifndef TRADITIONAL_LOCK
                if (slot.is_tombstone() && slot.is_volatile()) {
                    slot.persist_and_clear();
                    continue;
                }
#endif
                involved_kv++;

                std::string_view k = slot->key();
                size_t hash_key = std::hash<std::string_view>{}(k);
                // shunt = bucket_index(hash_key, depth, 0, 0) & 1;

                shunt = (hash_key >> (64 - depth + to_split.diff -
                                      BUCKET_INDEX_BIT_NUM - 1)) &
                        1;
                // fmt::print("shunt: {}\n", shunt);
                copied_slot = kv_ptr<KV>{slot.offset, 0, 0, slot.fingerprint};
                dst_in_cache[shunt].slots[slot_cnt[shunt]++] = copied_slot;
            }
            if (i < BUCKET_NUM_PER_SEGMENT / 2) {
                pmem_memcpy_persist(&dst[0]->buckets[i * 2], &dst_in_cache,
                                    sizeof(dst_in_cache));
            } else {
                pmem_memcpy_persist(
                        &dst[1]->buckets[(i - BUCKET_NUM_PER_SEGMENT / 2) * 2],
                        &dst_in_cache, sizeof(dst_in_cache));
            }
#ifndef TRADITIONAL_LOCK
            pmem_persist(&src_segment->buckets[i],
                         sizeof(src_segment->buckets[i]));
#endif
        }


        // copy to the new bottom level
        --depth;
        src_segment = to_split.get(1);
        for (size_t i = base; i < base + BUCKET_NUM_PER_SEGMENT / 2; i++) {
            memset(&dst_in_cache, 0, sizeof(dst_in_cache));
            unsigned slot_cnt[2] = {0, 0};
            for (size_t j = 0; j < KV_NUM_PER_BUCKET; j++) {
                unsigned shunt;
                kv_ptr<KV> copied_slot;
                kv_ptr<KV> &slot = src_segment->buckets[i].slots[j];
#ifndef TRADITIONAL_LOCK
                slot.set_copied();
#endif
                if (slot == nullptr) { continue; }
#ifndef TRADITIONAL_LOCK
                if (slot.is_tombstone() && slot.is_volatile()) {
                    slot.persist_and_clear();
                    continue;
                }
#endif
                involved_kv++;

                std::string_view k = slot->key();
                size_t hash_key = std::hash<std::string_view>{}(k);
                // shunt = bucket_index(hash_key, depth, 0, 1) & 1;
                // fmt::print("shunt: {}\n", shunt);
                shunt = (hash_key >> (64 - depth + to_split.diff -
                                      BUCKET_INDEX_BIT_NUM - 1)) &
                        1;

                copied_slot = kv_ptr<KV>{slot.offset, 0, 0, slot.fingerprint};
                dst_in_cache[shunt].slots[slot_cnt[shunt]++] = copied_slot;
            }
            pmem_memcpy_persist(&dst[2]->buckets[(i - base) * 2], &dst_in_cache,
                                sizeof(dst_in_cache));
#ifndef TRADITIONAL_LOCK
            pmem_persist(&src_segment->buckets[i],
                         sizeof(src_segment->buckets[i]));
#endif
        }
#ifdef PMHB_LATENCY
        pmhb_ns::sample_guard<steph<KV>, pmhb_ns::RESIZE_ITEM_NUMBER>{
                involved_kv};
#endif
        return std::tuple{
                segment_ptr<KV>(off0, off2, 0,
                                to_split.diff ? to_split.diff - 1 : 0),
                segment_ptr<KV>(off1, off2, 0,
                                to_split.diff ? to_split.diff - 1 : 0)};
    }

    void recover() {
        /* unlock */
        for (size_t i = 0; i < dir->capacity; i++) { dir->cur[i].lck = 0; }
        pmem_persist(dir->cur.get(), sizeof(dir->cur[0]) * dir->capacity);
        add_write_counter<KV>(sizeof(dir->cur[0]) * dir->capacity);
        /* go ahead with directory double */
        if (dir->resizing) { hidden_worker.submit_flush_dir_request(dir); }
    }

    size_t get_memory_usage() {
        while (hidden_worker.dir_need_double) {
            /* waiting for the background end */
        }
        /* traverse all structure */
        segment_ptr<KV> last_pointer;
        size_t sum = 0;

        const auto &d = *(dir.get());

        for (size_t i = 0; i < d.capacity; i++) {
            if (last_pointer.offset0 == d.cur[i].offset0) {
                continue;
            } else {
                sum += sizeof(Segment<KV>);
                if (last_pointer.offset1 != d.cur[i].offset1) {
                    sum += sizeof(Segment<KV>);
                }
                last_pointer = d.cur[i];
            }
        }
        /* Directory */
        sum += sizeof(segment_ptr<KV>) * d.capacity * 3 + sizeof(Directory<KV>);
        return sum;
    }
};// namespace steph_ns

}// namespace steph_ns

#endif//STEPH_STEPH_HPP