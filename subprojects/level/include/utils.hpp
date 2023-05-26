
// Copyright (c) Simon Fraser University & The Chinese University of Hong Kong. All rights reserved.
// Licensed under the MIT license.
#pragma once
#ifndef PMEM
#define PMEM
#endif

#include <immintrin.h>
#include <stdint.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <time.h>

#include <cstdint>
#include <fmt/core.h>
#include <fmt/ostream.h>
#include <iostream>
#ifdef PMEM
#include "libpmem.h"
#include "libpmemobj.h"
#endif

namespace level_ns {

inline static constexpr const uint32_t kCacheLineSize = 64;

inline static bool FileExists(const char *pool_path) {
    struct stat buffer;
    return (stat(pool_path, &buffer) == 0);
}

#ifdef PMEM
#define CREATE_MODE_RW (S_IWUSR | S_IRUSR)
#endif


// ADD and SUB return the value after add or sub
#define ADD(_p, _v) (__atomic_add_fetch(_p, _v, __ATOMIC_SEQ_CST))
#define SUB(_p, _v) (__atomic_sub_fetch(_p, _v, __ATOMIC_SEQ_CST))
#define LOAD(_p) (__atomic_load_n(_p, __ATOMIC_SEQ_CST))
#define STORE(_p, _v) (__atomic_store_n(_p, _v, __ATOMIC_SEQ_CST))

#define SIMD 1
#define SIMD_CMP8(src, key)                                                    \
    do {                                                                       \
        const __m256i key_data = _mm256_set1_epi8(key);                        \
        __m256i seg_data =                                                     \
                _mm256_loadu_si256(reinterpret_cast<const __m256i *>(src));    \
        __m256i rv_mask = _mm256_cmpeq_epi8(seg_data, key_data);               \
        mask = _mm256_movemask_epi8(rv_mask);                                  \
    } while (0)

#define SSE_CMP8(src, key)                                                     \
    do {                                                                       \
        const __m128i key_data = _mm_set1_epi8(key);                           \
        __m128i seg_data =                                                     \
                _mm_loadu_si128(reinterpret_cast<const __m128i *>(src));       \
        __m128i rv_mask = _mm_cmpeq_epi8(seg_data, key_data);                  \
        mask = _mm_movemask_epi8(rv_mask);                                     \
    } while (0)


inline void mfence(void) { asm volatile("mfence" ::: "memory"); }

inline int msleep(uint64_t msec) {
    struct timespec ts;
    int res;

    ts.tv_sec = msec / 1000;
    ts.tv_nsec = (msec % 1000) * 1000000;

    do { res = nanosleep(&ts, &ts); } while (res && errno == EINTR);

    return res;
}

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

}// namespace level_ns