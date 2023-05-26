#ifndef STEPH_UTIL_HPP
#define STEPH_UTIL_HPP
#include <chrono>
#include <cstdarg>
#include <fmt/core.h>
#include <fmt/ostream.h>
#include <sched.h>
#include <string_view>

#if defined PMHB_LATENCY || defined COUNTING_WRITE
#include "../../include/sample_guard.hpp"
#endif

// timer
namespace steph_ns {

template<typename KV>
struct steph;

template<typename KV>
void add_write_counter(size_t size) {
#if defined(COUNTING_WRITE)
    pmhb_ns::sample_guard<steph<KV>, pmhb_ns::WRITE_COUNT>{size};
#endif
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

void pin_thread(size_t id) {
    cpu_set_t mask;
    cpu_set_t get;
    CPU_ZERO(&mask);
    CPU_SET(id, &mask);
    if (sched_setaffinity(0, sizeof(mask), &mask) == -1) {
        fmt::print("[FAIL] cannot set affinity\n");
    }

    CPU_ZERO(&get);
    if (sched_getaffinity(0, sizeof(get), &get) == -1) {
        fmt::print("[FAIL] cannot get affinity\n");
    }
}

void pre_fault(void *pm, size_t len, size_t granularity = 2ul << 20) {
#ifdef PREFAULT
    auto light_pin_thread = [](size_t id) {
        cpu_set_t mask;
        cpu_set_t get;
        CPU_ZERO(&mask);
        CPU_SET(id, &mask);
        if (sched_setaffinity(0, sizeof(mask), &mask) == -1) {
            fmt::print("[FAIL] cannot set affinity\n");
        }

        CPU_ZERO(&get);
        if (sched_getaffinity(0, sizeof(get), &get) == -1) {
            fmt::print("[FAIL] cannot get affinity\n");
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
                fmt::print("{} total {:.2f}ms [level1]\n", s, t.get() / 1e6);
            } else if (t.get() > 50'000) {
                fmt::print("{} total {:.2f}us [level2]\n", s, t.get() / 1e3);
            } else {
                fmt::print("{} total {:.2f}us [level3]\n", s, t.get() / 1e3);
            }
        }
    }
};

#define myLOG fmt::print
#define myLOG_DEBUG fmt::print
}// namespace steph_ns

#endif//STEPH_UTIL_HPP
