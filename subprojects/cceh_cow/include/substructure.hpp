#ifndef CCEH_COW_SUBSTRUCTURE_H
#define CCEH_COW_SUBSTRUCTURE_H

#include <cstdlib>
#include <fmt/core.h>
#include <fmt/ostream.h>
#include <immintrin.h>
#include <iostream>
#include <string_view>
// #define likely(x) __builtin_expect((x), 1)
// #define unlikely(x) __builtin_expect((x), 0)

namespace cceh_cow_ns {

typedef const char *Value_t;

inline constexpr size_t SENTINEL = -2;// 11111...110
inline constexpr size_t INVALID = -1; // 11111...111

inline constexpr Value_t NONE = 0x0;
inline const Value_t DEFAULT = reinterpret_cast<Value_t>(1);


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
        // if (offset > 169ul << 30) {
        //     fmt::print("Out of border with offset {}, with uulo {:x}, from src "
        //                "{}\n",
        //                offset, pool_uuid_lo, src);
        //     exit(1);
        // }
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

class Timer {
private:
    /* data */
    std::chrono::steady_clock::time_point start_time, end_time;

public:
    void start() { start_time = std::chrono::steady_clock::now(); }
    void end() { end_time = std::chrono::steady_clock::now(); }
    auto get() {
        return std::chrono::nanoseconds(end_time - start_time).count();
    }
};
struct time_guard {
    // #ifdef DEBUG
    Timer t;
    std::string s;
    time_guard *ptg;
    // #endif
    time_guard(std::string_view s_) {
        // #ifdef DEBUG
        ptg = nullptr;
        s += s_;
        t.start();
        // #endif
    }
    time_guard(std::string_view s_, time_guard &tg) {
        // #ifdef DEBUG
        ptg = &tg;
        s += s_;
        t.start();
        // #endif
    }

    ~time_guard() {
        // #ifdef DEBUG
        t.end();
        if (ptg) {
            std::string s_;
            if (t.get() > 1'000'000) {
                s_ = fmt::format("<{} : {:.2f} ms!!> ", s, t.get() / 1e6);
            } else if (t.get() > 5'000) {
                s_ = fmt::format("<{} : {:.2f} us!> ", s, t.get() / 1e3);
            } else {
                s_ = fmt::format("<{} : {} ns> ", s, t.get());
            }
            ptg->s += s_;
        } else {
            if (t.get() > 1'000'000) {
                fmt::print("[important] {} totally uses {:.2f}ms [level1] \n",
                           s, t.get() / 1e6);
            } else if (t.get() > 50'000) {
                fmt::print("{} totally uses {:.2f}us [level2] \n", s,
                           t.get() / 1e3);
            } else {
                fmt::print("{} totally uses {:.2f}us [level3] \n", s,
                           t.get() / 1e3);
            }
        }
    }
};

#define CC_LOG fmt::print

}// namespace cceh_cow_ns

#endif// CCEH_COW_SUBSTRUCTURE_H
