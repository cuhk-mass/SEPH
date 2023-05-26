#ifndef DASH_SUBSTRUCTURE_H
#define DASH_SUBSTRUCTURE_H

#include "allocator.hpp"
#include "hash.hpp"
#include "utils.hpp"
#include <iostream>

namespace dash_ns {

typedef size_t Key_t;
typedef const char *Value_t;

inline constexpr Key_t SENTINEL = -2;// 11111...110
inline constexpr Key_t INVALID = -1; // 11111...111

inline constexpr Value_t NONE = 0x0;
inline const Value_t DEFAULT = reinterpret_cast<Value_t>(1);


inline uint64_t merge_time;
//#define COUNTING 1
//#define PREALLOC 1
template<typename KV>
class Finger_EH;


inline const uint32_t lockSet = ((uint32_t) 1 << 31);
inline const uint32_t lockMask = ((uint32_t) 1 << 31) - 1;
inline const int overflowSet = 1 << 4;
inline const int countMask = (1 << 4) - 1;
inline const uint64_t tailMask = (1UL << 56) - 1;
inline const uint64_t headerMask = ((1UL << 8) - 1) << 56;
inline const uint8_t overflowBitmapMask = (1 << 4) - 1;

inline constexpr size_t k_PairSize = 16;// a k-v _Pair with a bit
inline constexpr size_t kNumPairPerBucket =
        14; /* it is determined by the usage of the fingerprint*/
inline constexpr size_t kFingerBits = 8;
inline constexpr size_t kMask = (1 << kFingerBits) - 1;
inline const constexpr size_t kNumBucket =
        64; /* the number of normal buckets in one segment*/
inline constexpr size_t stashBucket =
        2; /* the number of stash buckets in one segment*/
inline constexpr int allocMask = (1 << kNumPairPerBucket) - 1;
inline constexpr size_t bucketMask = ((1 << (int) log2(kNumBucket)) - 1);
inline constexpr size_t stashMask = (1 << (int) log2(stashBucket)) - 1;
inline constexpr uint8_t stashHighMask = ~((uint8_t) stashMask);

#define BUCKET_INDEX(hash) ((hash >> kFingerBits) & bucketMask)
#define GET_COUNT(var) ((var) &countMask)
#define GET_MEMBER(var) (((var) >> 4) & allocMask)
#define GET_INVERSE_MEMBER(var) ((~((var) >> 4)) & allocMask)
#define GET_BITMAP(var) ((var) >> 18)


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
        return reinterpret_cast<pointer>(
                pmemobj_direct({pool_uuid_lo, offset}));
    }

    bool cas(c_ptr const &expeceted, c_ptr const &desired) {
        return __sync_bool_compare_and_swap(&offset, expeceted.offset,
                                            desired.offset);
    }

    /* Operators */
    c_ptr &operator=(size_t n) {
        offset = n;
        return *this;
    }
    bool operator==(size_t n) { return offset == n; }
    bool operator!=(size_t n) { return offset != n; }
    pointer operator->() const noexcept { return get(); }
    reference operator*() const { return *get(); }
    reference operator[](size_t idx) const { return *(get() + idx); }
    bool operator==(std::nullptr_t) const noexcept { return offset == 0; }
    bool operator!=(std::nullptr_t) const noexcept {
        return !(*this == nullptr);// NOLINT
    }
};


template<class KV>
struct _Pair {
    c_ptr<typename KV::K_TYPE> key;
    c_ptr<typename KV::V_TYPE> value;
};

template<class KV>
struct Bucket {
    /* For debug */
    void list() {
        printf("bit map is %x\n", GET_BITMAP(bitmap));
        auto t = GET_BITMAP(bitmap);
        for (int i = 0; i < 14; i++) {
            if (CHECK_BIT(t, i)) {
                printf("%d with offset %lu, ", i, (size_t) _[i].key.offset);
            }
        }
        std::cout << std::endl;
    }


    inline void write_slot(int slot, std::string_view key,
                           std::string_view value,
                           c_ptr<typename KV::K_TYPE> k_ptr,
                           c_ptr<typename KV::V_TYPE> v_ptr,
                           bool flush = true) {
        PMEMoid p_kv;
        if (k_ptr == 0 || v_ptr == 0) {
            // std::cout << "[debug] allocated " << k_ptr.offset << ' '
            //           << v_ptr.offset << std::endl;
#ifdef WRITE_KV
            auto callback = [](PMEMobjpool *pool, void *ptr, void *arg) {
                auto [k_size, key, v_size, value] =
                        *(reinterpret_cast<
                                std::tuple<size_t, std::string_view, size_t,
                                           std::string_view> *>(arg));
                new (ptr) KV(k_size, key.data(), v_size, value.data());
                return 0;
            };
            std::tuple callback_args = {16, key, 16, value};
            Allocator::Allocate(&p_kv, 0, 40, callback,
                                reinterpret_cast<void *>(&callback_args));
#endif
            _[slot].key = c_ptr<typename KV::K_TYPE>(p_kv.off + sizeof(KV));
            _[slot].value = c_ptr<typename KV::V_TYPE>(
                    p_kv.off + sizeof(typename KV::K_TYPE) + sizeof(KV));
            // fmt::print("written kv with k {}, v {}",
            //              (char const *) _[slot].key->key, _[slot].value);
            // LOG("written kv k " << (void *) _[slot].key << " "
            //                     << (char const *) _[slot].key->key
            //                     << "\n           v " << (void *) _[slot].value
            //                     << " " << _[slot].value);
        } else {
            _[slot].key = k_ptr;
            _[slot].value = v_ptr;
        }

#ifdef PMEM
        if (flush) { Allocator::Persist<KV>(&_[slot], sizeof(_[slot])); }
#endif
    }

    inline int find_empty_slot() {
        if (GET_COUNT(bitmap) == kNumPairPerBucket) { return -1; }
        auto mask = ~(GET_BITMAP(bitmap));
        return __builtin_ctz(mask);
    }

    /*true indicates overflow, needs extra check in the stash*/
    inline bool test_overflow() { return overflowCount; }

    inline bool test_stash_check() { return (overflowBitmap & overflowSet); }

    inline void clear_stash_check() {
        overflowBitmap = overflowBitmap & (~overflowSet);
    }

    inline void set_indicator(uint8_t meta_hash, Bucket<KV> *neighbor,
                              uint8_t pos) {
        int mask = overflowBitmap & overflowBitmapMask;
        mask = ~mask;
        auto index = __builtin_ctz(mask);

        if (index < 4) {
            finger_array[14 + index] = meta_hash;
            overflowBitmap = ((uint8_t) (1 << index) | overflowBitmap);
            overflowIndex = (overflowIndex & (~(3 << (index * 2)))) |
                            (pos << (index * 2));
        } else {
            mask = neighbor->overflowBitmap & overflowBitmapMask;
            mask = ~mask;
            index = __builtin_ctz(mask);
            if (index < 4) {
                neighbor->finger_array[14 + index] = meta_hash;
                neighbor->overflowBitmap =
                        ((uint8_t) (1 << index) | neighbor->overflowBitmap);
                neighbor->overflowMember =
                        ((uint8_t) (1 << index) | neighbor->overflowMember);
                neighbor->overflowIndex =
                        (neighbor->overflowIndex & (~(3 << (index * 2)))) |
                        (pos << (index * 2));
            } else { /*overflow, increase count*/
                overflowCount++;
            }
        }
        overflowBitmap = overflowBitmap | overflowSet;
    }

    /*both clear this bucket and its neighbor bucket*/
    inline void unset_indicator(uint8_t meta_hash, Bucket<KV> *neighbor,
                                uint64_t pos) {
        /*also needs to ensure that this meta_hash must belongs to other bucket*/
        bool clear_success = false;
        int mask1 = overflowBitmap & overflowBitmapMask;
        for (int i = 0; i < 4; ++i) {
            if (CHECK_BIT(mask1, i) && (finger_array[14 + i] == meta_hash) &&
                (((1 << i) & overflowMember) == 0) &&
                (((overflowIndex >> (2 * i)) & stashMask) == pos)) {
                overflowBitmap = overflowBitmap & ((uint8_t) (~(1 << i)));
                overflowIndex = overflowIndex & (~(3 << (i * 2)));
                assert(((overflowIndex >> (i * 2)) & stashMask) == 0);
                clear_success = true;
                break;
            }
        }

        int mask2 = neighbor->overflowBitmap & overflowBitmapMask;
        if (!clear_success) {
            for (int i = 0; i < 4; ++i) {
                if (CHECK_BIT(mask2, i) &&
                    (neighbor->finger_array[14 + i] == meta_hash) &&
                    (((1 << i) & neighbor->overflowMember) != 0) &&
                    (((neighbor->overflowIndex >> (2 * i)) & stashMask) ==
                     pos)) {
                    neighbor->overflowBitmap =
                            neighbor->overflowBitmap & ((uint8_t) (~(1 << i)));
                    neighbor->overflowMember =
                            neighbor->overflowMember & ((uint8_t) (~(1 << i)));
                    neighbor->overflowIndex =
                            neighbor->overflowIndex & (~(3 << (i * 2)));
                    assert(((neighbor->overflowIndex >> (i * 2)) & stashMask) ==
                           0);
                    clear_success = true;
                    break;
                }
            }
        }

        if (!clear_success) { overflowCount--; }

        mask1 = overflowBitmap & overflowBitmapMask;
        mask2 = neighbor->overflowBitmap & overflowBitmapMask;
        if (((mask1 & (~overflowMember)) == 0) && (overflowCount == 0) &&
            ((mask2 & neighbor->overflowMember) == 0)) {
            clear_stash_check();
        }
    }

    int unique_check(uint8_t meta_hash, std::string_view key,
                     Bucket<KV> *neighbor, Bucket<KV> *stash) {
        if ((check_and_get(meta_hash, key, false) != NONE) ||
            (neighbor->check_and_get(meta_hash, key, true) != NONE)) {
            return -1;
        }

        if (test_stash_check()) {
            auto test_stash = false;
            if (test_overflow()) {
                test_stash = true;
            } else {
                int mask = overflowBitmap & overflowBitmapMask;
                if (mask != 0) {
                    for (int i = 0; i < 4; ++i) {
                        if (CHECK_BIT(mask, i) &&
                            (finger_array[14 + i] == meta_hash) &&
                            (((1 << i) & overflowMember) == 0)) {
                            test_stash = true;
                            goto STASH_CHECK;
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
        STASH_CHECK:
            if (test_stash == true) {
                for (int i = 0; i < stashBucket; ++i) {
                    Bucket *curr_bucket = stash + i;
                    if (curr_bucket->check_and_get(meta_hash, key, false) !=
                        NONE) {
                        return -1;
                    }
                }
            }
        }
        return 0;
    }

    inline int get_current_mask() {
        int mask = GET_BITMAP(bitmap) & GET_INVERSE_MEMBER(bitmap);
        return mask;
    }

    Value_t check_and_get(uint8_t meta_hash, std::string_view key, bool probe) {
        int mask = 0;
        SSE_CMP8(finger_array, meta_hash);
        if (!probe) {
            mask = mask & GET_BITMAP(bitmap) & (~GET_MEMBER(bitmap));
        } else {
            mask = mask & GET_BITMAP(bitmap) & GET_MEMBER(bitmap);
        }

        if (mask == 0) { return NONE; }


        /* variable-length key*/
        for (int i = 0; i < 14; i += 1) {
            if (CHECK_BIT(mask, i) && (_[i].key->data() == key)) {
                return _[i].value->data();
            }
        }

        return NONE;
    }

    inline void set_hash(int index, uint8_t meta_hash, bool probe) {
        finger_array[index] = meta_hash;
        uint32_t new_bitmap = bitmap | (1 << (index + 18));
        if (probe) { new_bitmap = new_bitmap | (1 << (index + 4)); }
        new_bitmap += 1;
        bitmap = new_bitmap;
    }

    inline uint8_t get_hash(int index) { return finger_array[index]; }

    inline void unset_hash(int index, bool nt_flush = false) {
        uint32_t new_bitmap =
                bitmap & (~(1 << (index + 18))) & (~(1 << (index + 4)));
        assert(GET_COUNT(bitmap) <= kNumPairPerBucket);
        assert(GET_COUNT(bitmap) > 0);
        new_bitmap -= 1;
#ifdef PMEM
        if (nt_flush) {
            Allocator::NTWrite32(reinterpret_cast<uint32_t *>(&bitmap),
                                 new_bitmap);
        } else {
            bitmap = new_bitmap;
        }
#else
        bitmap = new_bitmap;
#endif
    }

    inline void get_lock() {
        uint32_t new_value = 0;
        uint32_t old_value = 0;
        do {
            while (true) {
                old_value = __atomic_load_n(&version_lock, __ATOMIC_ACQUIRE);
                if (!(old_value & lockSet)) {
                    old_value &= lockMask;
                    break;
                }
            }
            new_value = old_value | lockSet;
        } while (!DASH_CAS(&version_lock, &old_value, new_value));
    }

    inline bool try_get_lock() {
        uint32_t v = __atomic_load_n(&version_lock, __ATOMIC_ACQUIRE);
        if (v & lockSet) { return false; }
        auto old_value = v & lockMask;
        auto new_value = v | lockSet;
        return DASH_CAS(&version_lock, &old_value, new_value);
    }

    inline void release_lock() {
        uint32_t v = version_lock;
        __atomic_store_n(&version_lock, v + 1 - lockSet, __ATOMIC_RELEASE);
    }

    /*if the lock is set, return true*/
    inline bool test_lock_set(uint32_t &version) {
        version = __atomic_load_n(&version_lock, __ATOMIC_ACQUIRE);
        return (version & lockSet) != 0;
    }

    // test whether the version has change, if change, return true
    inline bool test_lock_version_change(uint32_t old_version) {
        auto value = __atomic_load_n(&version_lock, __ATOMIC_ACQUIRE);
        return (old_version != value);
    }

    int Insert(std::string_view key, std::string_view value, uint8_t meta_hash,
               bool probe, c_ptr<typename KV::K_TYPE> k_ptr,
               c_ptr<typename KV::V_TYPE> v_ptr) {
        auto slot = find_empty_slot();
        assert(slot < kNumPairPerBucket);
        if (slot == -1) { return -1; }
        // fmt::print("normal write slot in bucket {}", key);
        write_slot(slot, key, value, k_ptr, v_ptr);
        // fmt::print("_[slot]. key is {}", _[slot].key->data());
        set_hash(slot, meta_hash, probe);
        return 0;
    }

    int Insert(c_ptr<typename KV::K_TYPE> key, c_ptr<typename KV::V_TYPE> value,
               uint8_t meta_hash, bool probe) {
        auto slot = find_empty_slot();
        assert(slot < kNumPairPerBucket);
        if (slot == -1) { return -1; }
        _[slot].key = key;
        _[slot].value = value;

        set_hash(slot, meta_hash, probe);
        return 0;
    }


    /*if delete success, then return 0, else return -1*/
    int Update(uint8_t meta_hash, bool probe, std::string_view key,
               c_ptr<typename KV::K_TYPE> k_ptr,
               c_ptr<typename KV::V_TYPE> v_ptr) {
        int mask = 0;
        SSE_CMP8(finger_array, meta_hash);
        if (!probe) {
            mask = mask & GET_BITMAP(bitmap) & (~GET_MEMBER(bitmap));
        } else {
            mask = mask & GET_BITMAP(bitmap) & GET_MEMBER(bitmap);
        }

        if (mask == 0) { return -1; }


        /* variable-length key*/
        for (int i = 0; i < 14; i += 1) {
            if (CHECK_BIT(mask, i) && (_[i].key->data() == key)) {
                _[i].value = v_ptr;
                // _[i].key = k_ptr;
#ifdef PMEM
                Allocator::Persist<KV>(&_[i], sizeof(_[i]));
#endif
                return 0;
            }
        }

        return -1;
    }

    /*if delete success, then return 0, else return -1*/
    int Delete(std::string_view key, uint8_t meta_hash, bool probe) {
        /*do the simd and check the key, then do the delete operation*/
        int mask = 0;
        SSE_CMP8(finger_array, meta_hash);
        if (!probe) {
            mask = mask & GET_BITMAP(bitmap) & (~GET_MEMBER(bitmap));
        } else {
            mask = mask & GET_BITMAP(bitmap) & GET_MEMBER(bitmap);
        }

        /*loop unrolling*/
        if (mask != 0) {
            for (int i = 0; i < 12; i += 4) {
                if (CHECK_BIT(mask, i) && (_[i].key->data() == key)) {
                    unset_hash(i, false);
                    return 0;
                }

                if (CHECK_BIT(mask, i + 1) && (_[i + 1].key->data() == key)) {
                    unset_hash(i + 1, false);
                    return 0;
                }

                if (CHECK_BIT(mask, i + 2) && (_[i + 2].key->data() == key)) {
                    unset_hash(i + 2, false);
                    return 0;
                }

                if (CHECK_BIT(mask, i + 3) && (_[i + 3].key->data() == key)) {
                    unset_hash(i + 3, false);
                    return 0;
                }
            }

            if (CHECK_BIT(mask, 12) && (_[12].key->data() == key)) {
                unset_hash(12, false);
                return 0;
            }

            if (CHECK_BIT(mask, 13) && (_[13].key->data() == key)) {
                unset_hash(13, false);
                return 0;
            }
        }

        return -1;
    }

    int Insert_with_noflush(c_ptr<typename KV::K_TYPE> key,
                            c_ptr<typename KV::V_TYPE> value, uint8_t meta_hash,
                            bool probe) {
        auto slot = find_empty_slot();
        /* this branch can be removed*/
        assert(slot < kNumPairPerBucket);
        if (slot == -1) {
            std::cout << "Cannot find the empty slot, for key " << key->data()
                      << std::endl;
            return -1;
        }
        _[slot].value = value;
        _[slot].key = key;
        set_hash(slot, meta_hash, probe);
        return 0;
    }

    void Insert_displace(std::string_view key, std::string_view value,
                         uint8_t meta_hash, int slot, bool probe,
                         c_ptr<typename KV::K_TYPE> k_ptr,
                         c_ptr<typename KV::V_TYPE> v_ptr) {
        write_slot(slot, key, value, k_ptr, v_ptr, true);
        set_hash(slot, meta_hash, probe);
    }

    void Insert_displace_with_noflush(c_ptr<typename KV::K_TYPE> key,
                                      c_ptr<typename KV::V_TYPE> value,
                                      uint8_t meta_hash, int slot, bool probe) {
        _[slot].value = value;
        _[slot].key = key;
        set_hash(slot, meta_hash, probe);
    }

    /* Find the displacment element in this bucket*/
    inline int Find_org_displacement() {
        uint32_t mask = GET_INVERSE_MEMBER(bitmap);
        if (mask == 0) { return -1; }
        return __builtin_ctz(mask);
    }

    /*find element that it is in the probe*/
    inline int Find_probe_displacement() {
        uint32_t mask = GET_MEMBER(bitmap);
        if (mask == 0) { return -1; }
        return __builtin_ctz(mask);
    }

    inline void resetLock() { version_lock = 0; }

    inline void resetOverflowFP() {
        overflowBitmap = 0;
        overflowIndex = 0;
        overflowMember = 0;
        overflowCount = 0;
        clear_stash_check();
    }

    uint32_t version_lock;
    uint32_t bitmap;// allocation bitmap + pointer bitmap + counter
    uint8_t finger_array
            [18]; /*only use the first 14 bytes, can be accelerated by
                               SSE instruction,0-13 for finger, 14-17 for
                               overflowed*/
    uint8_t overflowBitmap;
    uint8_t overflowIndex;
    uint8_t overflowMember; /*overflowmember indicates membership of the overflow
                             fingerprint*/
    uint8_t overflowCount;
    uint8_t unused[2];

    _Pair<KV> _[kNumPairPerBucket];
};

template<class KV>
struct Table;

template<class KV>
struct Directory {
    typedef Table<KV> *table_p;
    // typedef c_ptr<Table<KV>> table_p;
    uint32_t global_depth;
    uint32_t version;
    uint32_t depth_count;
    table_p _[0];

    Directory(size_t capacity, size_t _version) {
        version = _version;
        global_depth = static_cast<size_t>(log2(capacity));
        depth_count = 0;
    }

    static void New(PMEMoid *dir, size_t capacity, size_t version) {
#ifdef PMEM
        auto callback = [](PMEMobjpool *pool, void *ptr, void *arg) {
            auto value_ptr =
                    reinterpret_cast<std::tuple<size_t, size_t> *>(arg);
            auto dir_ptr = reinterpret_cast<Directory *>(ptr);
            dir_ptr->version = std::get<1>(*value_ptr);
            dir_ptr->global_depth =
                    static_cast<size_t>(log2(std::get<0>(*value_ptr)));
            size_t cap = std::get<0>(*value_ptr);
            pmemobj_persist(pool, dir_ptr,
                            sizeof(Directory<KV>) + sizeof(uint64_t) * cap);
            return 0;
        };
        std::tuple callback_args = {capacity, version};
        Allocator::Allocate(dir, kCacheLineSize,
                            sizeof(Directory<KV>) + sizeof(table_p) * capacity,
                            callback, reinterpret_cast<void *>(&callback_args));
#else
        Allocator::Allocate((void **) dir, kCacheLineSize,
                            sizeof(Directory<KV>));
        new (*dir) Directory(capacity, version, tables);
#endif
    }
};

/*thread local table allcoation pool*/
template<class KV>
struct TlsTablePool {
    static Table<KV> *all_tables;
    static PMEMoid p_all_tables;
    static std::atomic<uint32_t> all_allocated;
    static const uint32_t kAllTables = 327680;

    static void AllocateMore() {
        auto callback = [](PMEMobjpool *pool, void *ptr, void *arg) {
            return 0;
        };
        std::pair callback_para(0, nullptr);
        Allocator::Allocate(&p_all_tables, kCacheLineSize,
                            sizeof(Table<KV>) * kAllTables, callback,
                            reinterpret_cast<void *>(&callback_para));
        all_tables =
                reinterpret_cast<Table<KV> *>(pmemobj_direct(p_all_tables));
        memset((void *) all_tables, 0, sizeof(Table<KV>) * kAllTables);
        all_allocated = 0;
        printf("MORE ");
    }

    TlsTablePool() {}
    static void Initialize() { AllocateMore(); }

    Table<KV> *tables = nullptr;
    static const uint32_t kTables = 128;
    uint32_t allocated = kTables;

    void TlsPrepare() {
    retry:
        uint32_t n = all_allocated.fetch_add(kTables);
        if (n == kAllTables) {
            AllocateMore();
            abort();
            goto retry;
        }
        tables = all_tables + n;
        allocated = 0;
    }

    Table<KV> *Get() {
        if (allocated == kTables) { TlsPrepare(); }
        return &tables[allocated++];
    }
};

template<class KV>
std::atomic<uint32_t> TlsTablePool<KV>::all_allocated(0);
template<class KV>
Table<KV> *TlsTablePool<KV>::all_tables = nullptr;
template<class KV>
PMEMoid TlsTablePool<KV>::p_all_tables = OID_NULL;

/* the segment class*/
template<class KV>
struct Table {
    static void New(PMEMoid *tbl, size_t depth, PMEMoid pp) {
#ifdef PMEM
#ifdef PREALLOC
        thread_local TlsTablePool<KV> tls_pool;
        auto ptr = tls_pool.Get();
        ptr->local_depth = depth;
        ptr->next = pp;
        *tbl = pmemobj_oid(ptr);
#else
        auto callback = [](PMEMobjpool *pool, void *ptr, void *arg) {
            auto value_ptr =
                    reinterpret_cast<std::pair<size_t, PMEMoid> *>(arg);
            auto table_ptr = reinterpret_cast<Table<KV> *>(ptr);
            table_ptr->local_depth = value_ptr->first;
            table_ptr->next = value_ptr->second;
            table_ptr->state = -3; /*NEW*/
            memset(&table_ptr->lock_bit, 0, sizeof(PMEMmutex) * 2);

            int sumBucket = kNumBucket + stashBucket;
            for (int i = 0; i < sumBucket; ++i) {
                auto curr_bucket = table_ptr->bucket + i;
                memset(curr_bucket, 0, 64);
            }

            pmemobj_persist(pool, table_ptr, sizeof(Table<KV>));
            return 0;
        };
        std::pair callback_para(depth, pp);
        Allocator::Allocate(tbl, kCacheLineSize, sizeof(Table<KV>), callback,
                            reinterpret_cast<void *>(&callback_para));
#endif
#else
        Allocator::ZAllocate((void **) tbl, kCacheLineSize, sizeof(Table<KV>));
        (*tbl)->local_depth = depth;
        (*tbl)->next = pp;
#endif
    };
    ~Table(void) {}

    bool Acquire_and_verify(size_t _pattern) {
        bucket->get_lock();
        if (pattern != _pattern) {
            bucket->release_lock();
            return false;
        } else {
            return true;
        }
    }

    void Acquire_remaining_locks() {
        for (int i = 1; i < kNumBucket; ++i) {
            auto curr_bucket = bucket + i;
            curr_bucket->get_lock();
        }
    }

    void Release_all_locks() {
        for (int i = 0; i < kNumBucket; ++i) {
            auto curr_bucket = bucket + i;
            curr_bucket->release_lock();
        }
    }

    int Insert(std::string_view key, std::string_view value, size_t key_hash,
               uint8_t meta_hash, Directory<KV> **_dir,
               c_ptr<typename KV::K_TYPE> k_ptr,
               c_ptr<typename KV::V_TYPE> v_ptr);
    void Insert4split(c_ptr<typename KV::K_TYPE> key,
                      c_ptr<typename KV::V_TYPE> value, size_t key_hash,
                      uint8_t meta_hash);
    void Insert4splitWithCheck(c_ptr<typename KV::K_TYPE> key,
                               c_ptr<typename KV::V_TYPE> value,
                               size_t key_hash,
                               uint8_t meta_hash); /*with uniqueness check*/
    Table<KV> *Split(size_t);
    void HelpSplit(Table<KV> *);
    int Delete(std::string_view key, size_t key_hash, uint8_t meta_hash,
               Directory<KV> **_dir);

    int Next_displace(Bucket<KV> *target, Bucket<KV> *neighbor,
                      Bucket<KV> *next_neighbor, std::string_view key,
                      std::string_view value, uint8_t meta_hash,
                      c_ptr<typename KV::K_TYPE> k_ptr,
                      c_ptr<typename KV::V_TYPE> v_ptr) {
        int displace_index = neighbor->Find_org_displacement();
        if ((GET_COUNT(next_neighbor->bitmap) != kNumPairPerBucket) &&
            (displace_index != -1)) {
            next_neighbor->Insert(neighbor->_[displace_index].key,
                                  neighbor->_[displace_index].value,
                                  neighbor->finger_array[displace_index], true);
            next_neighbor->release_lock();
#ifdef PMEM
            Allocator::Persist<KV>(&next_neighbor->bitmap,
                                   sizeof(next_neighbor->bitmap));
#endif
            neighbor->unset_hash(displace_index);
            neighbor->Insert_displace(key, value, meta_hash, displace_index,
                                      true, k_ptr, v_ptr);
            neighbor->release_lock();
#ifdef PMEM
            Allocator::Persist<KV>(&neighbor->bitmap, sizeof(neighbor->bitmap));
#endif
            target->release_lock();
#ifdef COUNTING
            __sync_fetch_and_add(&number, 1);
#endif
            return 0;
        }
        return -1;
    }

    int Prev_displace(Bucket<KV> *target, Bucket<KV> *prev_neighbor,
                      Bucket<KV> *neighbor, std::string_view key,
                      std::string_view value, uint8_t meta_hash,
                      c_ptr<typename KV::K_TYPE> k_ptr,
                      c_ptr<typename KV::V_TYPE> v_ptr) {
        int displace_index = target->Find_probe_displacement();
        if ((GET_COUNT(prev_neighbor->bitmap) != kNumPairPerBucket) &&
            (displace_index != -1)) {
            prev_neighbor->Insert(target->_[displace_index].key,
                                  target->_[displace_index].value,
                                  target->finger_array[displace_index], false);
            prev_neighbor->release_lock();
#ifdef PMEM
            Allocator::Persist<KV>(&prev_neighbor->bitmap,
                                   sizeof(prev_neighbor->bitmap));
#endif
            target->unset_hash(displace_index);
            target->Insert_displace(key, value, meta_hash, displace_index,
                                    false, k_ptr, v_ptr);
            target->release_lock();
#ifdef PMEM
            Allocator::Persist<KV>(&target->bitmap, sizeof(target->bitmap));
#endif
            neighbor->release_lock();
#ifdef COUNTING
            __sync_fetch_and_add(&number, 1);
#endif
            return 0;
        }
        return -1;
    }

    /* this function in Insert and Insert4split */
    int Stash_insert(Bucket<KV> *target, Bucket<KV> *neighbor,
                     std::string_view key, std::string_view value,
                     uint8_t meta_hash, int stash_pos,
                     c_ptr<typename KV::K_TYPE> k_ptr,
                     c_ptr<typename KV::V_TYPE> v_ptr) {
        for (int i = 0; i < stashBucket; ++i) {
            Bucket<KV> *curr_bucket =
                    bucket + kNumBucket + ((stash_pos + i) & stashMask);
            if (GET_COUNT(curr_bucket->bitmap) < kNumPairPerBucket) {
                curr_bucket->Insert(key, value, meta_hash, false, k_ptr, v_ptr);
#ifdef PMEM
                Allocator::Persist<KV>(&curr_bucket->bitmap,
                                       sizeof(curr_bucket->bitmap));
#endif
                target->set_indicator(meta_hash, neighbor,
                                      (stash_pos + i) & stashMask);
#ifdef COUNTING
                __sync_fetch_and_add(&number, 1);
#endif
                return 0;
            }
        }
        return -1;
    }

    int Stash_insert(Bucket<KV> *target, Bucket<KV> *neighbor,
                     c_ptr<typename KV::K_TYPE> key,
                     c_ptr<typename KV::V_TYPE> value, uint8_t meta_hash,
                     int stash_pos) {
        for (int i = 0; i < stashBucket; ++i) {
            Bucket<KV> *curr_bucket =
                    bucket + kNumBucket + ((stash_pos + i) & stashMask);
            if (GET_COUNT(curr_bucket->bitmap) < kNumPairPerBucket) {
                curr_bucket->Insert(key, value, meta_hash, false);
#ifdef PMEM
                Allocator::Persist<KV>(&curr_bucket->bitmap,
                                       sizeof(curr_bucket->bitmap));
#endif
                target->set_indicator(meta_hash, neighbor,
                                      (stash_pos + i) & stashMask);
#ifdef COUNTING
                __sync_fetch_and_add(&number, 1);
#endif
                return 0;
            }
        }
        return -1;
    }

    void recoverMetadata() {
        Bucket<KV> *curr_bucket, *neighbor_bucket;
        /*reset the lock and overflow meta-data*/
        uint64_t knumber = 0;
        for (int i = 0; i < kNumBucket; ++i) {
            curr_bucket = bucket + i;
            curr_bucket->resetLock();
            curr_bucket->resetOverflowFP();
            neighbor_bucket = bucket + ((i + 1) & bucketMask);
            for (int j = 0; j < kNumPairPerBucket; ++j) {
                int mask = curr_bucket->get_current_mask();
                if (CHECK_BIT(mask, j) &&
                    (neighbor_bucket->check_and_get(
                             curr_bucket->finger_array[j],
                             curr_bucket->_[j].key->data(), true) != NONE)) {
                    curr_bucket->unset_hash(j);
                }
            }

#ifdef COUNTING
            knumber += __builtin_popcount(GET_BITMAP(curr_bucket->bitmap));
#endif
        }

        /*scan the stash buckets and re-insert the overflow FP to initial buckets*/
        for (int i = 0; i < stashBucket; ++i) {
            curr_bucket = bucket + kNumBucket + i;
            curr_bucket->resetLock();
#ifdef COUNTING
            knumber += __builtin_popcount(GET_BITMAP(curr_bucket->bitmap));
#endif
            uint64_t key_hash;
            auto mask = GET_BITMAP(curr_bucket->bitmap);
            for (int j = 0; j < kNumPairPerBucket; ++j) {
                if (CHECK_BIT(mask, j)) {
                    auto curr_key = curr_bucket->_[j].key;
                    key_hash = h(curr_key->data(), strlen(curr_key->data()));

                    /*compute the initial bucket*/
                    auto bucket_ix = BUCKET_INDEX(key_hash);
                    auto meta_hash =
                            ((uint8_t) (key_hash & kMask));// the last 8 bits
                    auto org_bucket = bucket + bucket_ix;
                    auto neighbor_bucket =
                            bucket + ((bucket_ix + 1) & bucketMask);
                    org_bucket->set_indicator(meta_hash, neighbor_bucket, i);
                }
            }
        }
#ifdef COUNTING
        number = knumber;
#endif
        /* No need to flush these meta-data because persistent or not does not
     * influence the correctness*/
    }

    char dummy[48];
    Bucket<KV> bucket[kNumBucket + stashBucket];
    size_t local_depth;
    size_t pattern;
    int number;
    PMEMoid next;
    int state; /*-1 means this bucket is merging, -2 means this bucket is
                splitting (SPLITTING), 0 meanning normal bucket, -3 means new
                bucket (NEW)*/
    PMEMmutex
            lock_bit; /* for the synchronization of the lazy recovery in one segment*/
};

/* it needs to verify whether this bucket has been deleted...*/
template<class KV>
int Table<KV>::Insert(std::string_view key, std::string_view value,
                      size_t key_hash, uint8_t meta_hash, Directory<KV> **_dir,
                      c_ptr<typename KV::K_TYPE> k_ptr,
                      c_ptr<typename KV::V_TYPE> v_ptr) {
RETRY:
    /*we need to first do the locking and then do the verify*/
    auto y = BUCKET_INDEX(key_hash);
    Bucket<KV> *target = bucket + y;
    Bucket<KV> *neighbor = bucket + ((y + 1) & bucketMask);
    target->get_lock();
    if (!neighbor->try_get_lock()) {
        target->release_lock();
        return -2;
    }

    auto old_sa = *_dir;
    auto x = (key_hash >> (8 * sizeof(key_hash) - old_sa->global_depth));
    if (reinterpret_cast<Table<KV> *>(reinterpret_cast<uint64_t>(old_sa->_[x]) &
                                      tailMask) != this) {
        neighbor->release_lock();
        target->release_lock();
        return -2;
    }

    /* unique check, needs to check 2 buckets */
    int ret;

    ret = target->unique_check(meta_hash, key, neighbor, bucket + kNumBucket);
    if (ret == -1) {
        neighbor->release_lock();
        target->release_lock();
        return -3; /* duplicate insert*/
    }

    if (((GET_COUNT(target->bitmap)) == kNumPairPerBucket) &&
        ((GET_COUNT(neighbor->bitmap)) == kNumPairPerBucket)) {
        Bucket<KV> *next_neighbor = bucket + ((y + 2) & bucketMask);
        // Next displacement
        if (!next_neighbor->try_get_lock()) {
            neighbor->release_lock();
            target->release_lock();
            return -2;
        }
        auto ret = Next_displace(target, neighbor, next_neighbor, key, value,
                                 meta_hash, k_ptr, v_ptr);
        if (ret == 0) { return 0; }
        next_neighbor->release_lock();

        Bucket<KV> *prev_neighbor;
        int prev_index;
        if (y == 0) {
            prev_neighbor = bucket + kNumBucket - 1;
            prev_index = kNumBucket - 1;
        } else {
            prev_neighbor = bucket + y - 1;
            prev_index = y - 1;
        }
        if (!prev_neighbor->try_get_lock()) {
            target->release_lock();
            neighbor->release_lock();
            return -2;
        }

        ret = Prev_displace(target, prev_neighbor, neighbor, key, value,
                            meta_hash, k_ptr, v_ptr);
        if (ret == 0) { return 0; }

        Bucket<KV> *stash = bucket + kNumBucket;
        if (!stash->try_get_lock()) {
            neighbor->release_lock();
            target->release_lock();
            prev_neighbor->release_lock();
            return -2;
        }
        ret = Stash_insert(target, neighbor, key, value, meta_hash,
                           y & stashMask, k_ptr, v_ptr);

        stash->release_lock();
        neighbor->release_lock();
        target->release_lock();
        prev_neighbor->release_lock();
        return ret;
    }

    /* the fp+bitmap are persisted after releasing the lock of one bucket but
   * still guarantee the correctness of avoidance of "use-before-flush" since
   * the search operation could only proceed only if both target bucket and
   * probe bucket are released
   */
    if (GET_COUNT(target->bitmap) <= GET_COUNT(neighbor->bitmap)) {
        // fmt::print("normal insert in Table {}", key);
        target->Insert(key, value, meta_hash, false, k_ptr, v_ptr);
        target->release_lock();
#ifdef PMEM
        Allocator::Persist<KV>(&target->bitmap, sizeof(target->bitmap));
#endif
        neighbor->release_lock();
    } else {
        neighbor->Insert(key, value, meta_hash, true, k_ptr, v_ptr);
        neighbor->release_lock();
#ifdef PMEM
        Allocator::Persist<KV>(&neighbor->bitmap, sizeof(neighbor->bitmap));
#endif
        target->release_lock();
    }
#ifdef COUNTING
    __sync_fetch_and_add(&number, 1);
#endif
    return 0;
}

/* function in split, no need to write KV */
template<class KV>
void Table<KV>::Insert4splitWithCheck(c_ptr<typename KV::K_TYPE> key,
                                      c_ptr<typename KV::V_TYPE> value,
                                      size_t key_hash, uint8_t meta_hash) {
    auto y = BUCKET_INDEX(key_hash);
    Bucket<KV> *target = bucket + y;
    Bucket<KV> *neighbor = bucket + ((y + 1) & bucketMask);
    auto ret = target->unique_check(meta_hash, key->data(), neighbor,
                                    bucket + kNumBucket);
    if (ret == -1) return;
    Bucket<KV> *insert_target;
    bool probe = false;
    if (GET_COUNT(target->bitmap) <= GET_COUNT(neighbor->bitmap)) {
        insert_target = target;
    } else {
        insert_target = neighbor;
        probe = true;
    }

    /*some bucket may be overflowed?*/
    if (GET_COUNT(insert_target->bitmap) < kNumPairPerBucket) {
        insert_target->_[GET_COUNT(insert_target->bitmap)].key = key;
        insert_target->_[GET_COUNT(insert_target->bitmap)].value = value;
        insert_target->set_hash(GET_COUNT(insert_target->bitmap), meta_hash,
                                probe);
#ifdef COUNTING
        ++number;
#endif
    } else {
        /*do the displacement or insertion in the stash*/
        Bucket<KV> *next_neighbor = bucket + ((y + 2) & bucketMask);
        int displace_index;
        displace_index = neighbor->Find_org_displacement();
        if (((GET_COUNT(next_neighbor->bitmap)) != kNumPairPerBucket) &&
            (displace_index != -1)) {
            next_neighbor->Insert_with_noflush(
                    neighbor->_[displace_index].key,
                    neighbor->_[displace_index].value,
                    neighbor->finger_array[displace_index], true);
            neighbor->unset_hash(displace_index);
            neighbor->Insert_displace_with_noflush(key, value, meta_hash,
                                                   displace_index, true);
#ifdef COUNTING
            ++number;
#endif
            return;
        }
        Bucket<KV> *prev_neighbor;
        int prev_index;
        if (y == 0) {
            prev_neighbor = bucket + kNumBucket - 1;
            prev_index = kNumBucket - 1;
        } else {
            prev_neighbor = bucket + y - 1;
            prev_index = y - 1;
        }

        displace_index = target->Find_probe_displacement();
        if (((GET_COUNT(prev_neighbor->bitmap)) != kNumPairPerBucket) &&
            (displace_index != -1)) {
            prev_neighbor->Insert_with_noflush(
                    target->_[displace_index].key,
                    target->_[displace_index].value,
                    target->finger_array[displace_index], false);
            target->unset_hash(displace_index);
            target->Insert_displace_with_noflush(key, value, meta_hash,
                                                 displace_index, false);
#ifdef COUNTING
            ++number;
#endif
            return;
        }

        Stash_insert(target, neighbor, key, value, meta_hash, y & stashMask);
    }
}

/*the insert needs to be perfectly balanced, not destory the power of balance*/
template<class KV>
void Table<KV>::Insert4split(c_ptr<typename KV::K_TYPE> key,
                             c_ptr<typename KV::V_TYPE> value, size_t key_hash,
                             uint8_t meta_hash) {
    auto y = BUCKET_INDEX(key_hash);
    Bucket<KV> *target = bucket + y;
    Bucket<KV> *neighbor = bucket + ((y + 1) & bucketMask);
    Bucket<KV> *insert_target;
    bool probe = false;
    if (GET_COUNT(target->bitmap) <= GET_COUNT(neighbor->bitmap)) {
        insert_target = target;
    } else {
        insert_target = neighbor;
        probe = true;
    }

    /*some bucket may be overflowed?*/
    if (GET_COUNT(insert_target->bitmap) < kNumPairPerBucket) {
        insert_target->_[GET_COUNT(insert_target->bitmap)].key = key;
        insert_target->_[GET_COUNT(insert_target->bitmap)].value = value;
        insert_target->set_hash(GET_COUNT(insert_target->bitmap), meta_hash,
                                probe);
#ifdef COUNTING
        ++number;
#endif
    } else {
        /*do the displacement or insertion in the stash*/
        Bucket<KV> *next_neighbor = bucket + ((y + 2) & bucketMask);
        int displace_index;
        displace_index = neighbor->Find_org_displacement();
        if (((GET_COUNT(next_neighbor->bitmap)) != kNumPairPerBucket) &&
            (displace_index != -1)) {
            next_neighbor->Insert_with_noflush(
                    neighbor->_[displace_index].key,
                    neighbor->_[displace_index].value,
                    neighbor->finger_array[displace_index], true);
            neighbor->unset_hash(displace_index);
            neighbor->Insert_displace_with_noflush(key, value, meta_hash,
                                                   displace_index, true);
#ifdef COUNTING
            ++number;
#endif
            return;
        }
        Bucket<KV> *prev_neighbor;
        int prev_index;
        if (y == 0) {
            prev_neighbor = bucket + kNumBucket - 1;
            prev_index = kNumBucket - 1;
        } else {
            prev_neighbor = bucket + y - 1;
            prev_index = y - 1;
        }

        displace_index = target->Find_probe_displacement();
        if (((GET_COUNT(prev_neighbor->bitmap)) != kNumPairPerBucket) &&
            (displace_index != -1)) {
            prev_neighbor->Insert_with_noflush(
                    target->_[displace_index].key,
                    target->_[displace_index].value,
                    target->finger_array[displace_index], false);
            target->unset_hash(displace_index);
            target->Insert_displace_with_noflush(key, value, meta_hash,
                                                 displace_index, false);
#ifdef COUNTING
            ++number;
#endif
            return;
        }

        /* for split ?*/
        Stash_insert(target, neighbor, key, value, meta_hash, y & stashMask);
    }
}

template<class KV>
void Table<KV>::HelpSplit(Table<KV> *next_table) {
    size_t new_pattern = (pattern << 1) + 1;
    size_t old_pattern = pattern << 1;

    size_t key_hash;
    uint32_t invalid_array[kNumBucket + stashBucket];
    for (int i = 0; i < kNumBucket; ++i) {
        auto *curr_bucket = bucket + i;
        auto mask = GET_BITMAP(curr_bucket->bitmap);
        uint32_t invalid_mask = 0;
        for (int j = 0; j < kNumPairPerBucket; ++j) {
            if (CHECK_BIT(mask, j)) {
                auto curr_key = curr_bucket->_[j].key;
                key_hash = h(curr_key->data(), strlen(curr_key->data()));


                if ((key_hash >> (64 - local_depth - 1)) == new_pattern) {
                    invalid_mask = invalid_mask | (1 << j);
                    next_table->Insert4splitWithCheck(
                            curr_bucket->_[j].key, curr_bucket->_[j].value,
                            key_hash, curr_bucket->finger_array[j]);
#ifdef COUNTING
                    number--;
#endif
                }
            }
        }
        invalid_array[i] = invalid_mask;
    }

    for (int i = 0; i < stashBucket; ++i) {
        auto *curr_bucket = bucket + kNumBucket + i;
        auto mask = GET_BITMAP(curr_bucket->bitmap);
        uint32_t invalid_mask = 0;
        for (int j = 0; j < kNumPairPerBucket; ++j) {
            if (CHECK_BIT(mask, j)) {

                auto curr_key = curr_bucket->_[j].key;
                key_hash = h(curr_key->data(), strlen(curr_key->data()));

                if ((key_hash >> (64 - local_depth - 1)) == new_pattern) {
                    invalid_mask = invalid_mask | (1 << j);
                    next_table->Insert4splitWithCheck(
                            curr_bucket->_[j].key, curr_bucket->_[j].value,
                            key_hash, curr_bucket->finger_array[j]);
                    auto bucket_ix = BUCKET_INDEX(key_hash);
                    auto org_bucket = bucket + bucket_ix;
                    auto neighbor_bucket =
                            bucket + ((bucket_ix + 1) & bucketMask);
                    org_bucket->unset_indicator(curr_bucket->finger_array[j],
                                                neighbor_bucket, i);
#ifdef COUNTING
                    number--;
#endif
                }
            }
        }
        invalid_array[kNumBucket + i] = invalid_mask;
    }
    next_table->pattern = new_pattern;
    Allocator::Persist<KV>(&next_table->pattern, sizeof(next_table->pattern));
    pattern = old_pattern;
    Allocator::Persist<KV>(&pattern, sizeof(pattern));

#ifdef PMEM
    Allocator::Persist<KV>(next_table, sizeof(Table));
    size_t sumBucket = kNumBucket + stashBucket;
    for (int i = 0; i < sumBucket; ++i) {
        auto curr_bucket = bucket + i;
        curr_bucket->bitmap = curr_bucket->bitmap &
                              (~(invalid_array[i] << 18)) &
                              (~(invalid_array[i] << 4));
        uint32_t count = __builtin_popcount(invalid_array[i]);
        curr_bucket->bitmap = curr_bucket->bitmap - count;
    }

    Allocator::Persist<KV>(this, sizeof(Table));
#endif
}

template<class KV>
Table<KV> *Table<KV>::Split(size_t _key_hash) {
#ifdef PMHB_LATENCY
    auto g = pmhb_ns::sample_guard<Finger_EH<KV>, pmhb_ns::REHASH>{};
#endif
    // LOG("thread " << pthread_self()
    //               << " starting to acquire locks for splitting "
    //               << (void *) this);


    size_t new_pattern = (pattern << 1) + 1;
    size_t old_pattern = pattern << 1;

    for (int i = 1; i < kNumBucket; ++i) { (bucket + i)->get_lock(); }
    state = -2; /*means the start of the split process*/
    Allocator::Persist<KV>(&state, sizeof(state));
    Table<KV>::New(&next, local_depth + 1, next);
    Table<KV> *next_table = reinterpret_cast<Table<KV> *>(pmemobj_direct(next));

    next_table->state = -2;
    Allocator::Persist<KV>(&next_table->state, sizeof(next_table->state));
    next_table->bucket
            ->get_lock(); /* get the first lock of the new bucket to avoid it
                 is operated(split or merge) by other threads*/
    // LOG("thread " << pthread_self() << " locks acquired for splitting "
    //               << (void *) this);
    size_t key_hash;
    uint32_t invalid_array[kNumBucket + stashBucket];
    size_t involved_kv = 0;
    for (int i = 0; i < kNumBucket; ++i) {
        auto *curr_bucket = bucket + i;
        auto mask = GET_BITMAP(curr_bucket->bitmap);
        uint32_t invalid_mask = 0;
        for (int j = 0; j < kNumPairPerBucket; ++j) {
            if (CHECK_BIT(mask, j)) {
                involved_kv++;

                auto curr_key = curr_bucket->_[j].key;
                key_hash = h(curr_key->data(), strlen(curr_key->data()));

                if ((key_hash >> (64 - local_depth - 1)) == new_pattern) {
                    invalid_mask = invalid_mask | (1 << j);
                    next_table->Insert4split(curr_bucket->_[j].key,
                                             curr_bucket->_[j].value, key_hash,
                                             curr_bucket->finger_array[j]);
                    /*this shceme may destory the balanced segment*/
                    // curr_bucket->unset_hash(j);
#ifdef COUNTING
                    number--;
#endif
                }
            }
        }
        invalid_array[i] = invalid_mask;
    }

    for (int i = 0; i < stashBucket; ++i) {
        auto *curr_bucket = bucket + kNumBucket + i;
        auto mask = GET_BITMAP(curr_bucket->bitmap);
        uint32_t invalid_mask = 0;
        for (int j = 0; j < kNumPairPerBucket; ++j) {
            if (CHECK_BIT(mask, j)) {
                involved_kv++;

                auto curr_key = curr_bucket->_[j].key;
                key_hash = h(curr_key->data(), strlen(curr_key->data()));
                if ((key_hash >> (64 - local_depth - 1)) == new_pattern) {
                    invalid_mask = invalid_mask | (1 << j);
                    next_table->Insert4split(
                            curr_bucket->_[j].key, curr_bucket->_[j].value,
                            key_hash,
                            curr_bucket->finger_array
                                    [j]); /*this shceme may destory the
                                                balanced segment*/
                    auto bucket_ix = BUCKET_INDEX(key_hash);
                    auto org_bucket = bucket + bucket_ix;
                    auto neighbor_bucket =
                            bucket + ((bucket_ix + 1) & bucketMask);
                    org_bucket->unset_indicator(curr_bucket->finger_array[j],
                                                neighbor_bucket, i);
#ifdef COUNTING
                    number--;
#endif
                }
            }
        }
        invalid_array[kNumBucket + i] = invalid_mask;
    }
#ifdef PMHB_LATENCY
    pmhb_ns::sample_guard<Finger_EH<KV>, pmhb_ns::RESIZE_ITEM_NUMBER>{
            involved_kv};
    /* One bucket in the new segment causes one write */
#endif
    next_table->pattern = new_pattern;
    Allocator::Persist<KV>(&next_table->pattern, sizeof(next_table->pattern));
    pattern = old_pattern;
    Allocator::Persist<KV>(&pattern, sizeof(pattern));

#ifdef PMEM
    Allocator::Persist<KV>(next_table, sizeof(Table));
    size_t sumBucket = kNumBucket + stashBucket;
    for (int i = 0; i < sumBucket; ++i) {
        auto curr_bucket = bucket + i;
        curr_bucket->bitmap = curr_bucket->bitmap &
                              (~(invalid_array[i] << 18)) &
                              (~(invalid_array[i] << 4));
        uint32_t count = __builtin_popcount(invalid_array[i]);
        curr_bucket->bitmap = curr_bucket->bitmap - count;
    }

    Allocator::Persist<KV>(this, sizeof(Table));
#endif

    // LOG("thread " << pthread_self() << " finished splitting " << (void *) this);
    return next_table;
}

}// namespace dash_ns


#endif//DASH_SUBSTRUCTURE_H