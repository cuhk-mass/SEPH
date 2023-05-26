#ifndef LEVEL_SUBSTRUCTURE_H
#define LEVEL_SUBSTRUCTURE_H

#include <cstdlib>
#include <immintrin.h>
#include <iostream>
#include <string_view>

#define BATCH

// #define likely(x) __builtin_expect((x), 1)
// #define unlikely(x) __builtin_expect((x), 0)
namespace level_ns {

typedef size_t Key_t;
typedef const char *Value_t;

inline constexpr Key_t SENTINEL = -2;// 11111...110
inline constexpr Key_t INVALID = -1; // 11111...111

inline constexpr Value_t NONE = 0x0;
inline const Value_t DEFAULT = reinterpret_cast<Value_t>(1);

/*variable length key*/
struct string_key {
    int length;
    char key[0];
    friend std::ostream &operator<<(std::ostream &stream, string_key &k) {
        stream << std::string_view{k.key, (size_t) k.length};
        return stream;
    }
};

struct Pair {
    Key_t key;
    Value_t value;

    Pair(void);

    Pair(Key_t _key, Value_t _value);

    Pair &operator=(const Pair &other);

    void *operator new(size_t size);

    void *operator new[](size_t size);
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
        return reinterpret_cast<pointer>(
                pmemobj_direct({pool_uuid_lo, offset}));
    }

    bool cas(c_ptr const &expeceted, c_ptr const &desired) {
        return __sync_bool_compare_and_swap(&offset, expeceted.offset,
                                            desired.offset);
    }

    /* Operators */
    pointer operator->() const noexcept { return get(); }
    reference operator*() const { return *get(); }
    reference operator[](size_t idx) const { return *(get() + idx); }
    bool operator==(std::nullptr_t) const noexcept { return offset == 0; }
    bool operator!=(std::nullptr_t) const noexcept {
        return !(*this == nullptr);// NOLINT
    }
};


}// namespace level_ns

#endif// LEVEL_SUBSTRUCTURE_H
