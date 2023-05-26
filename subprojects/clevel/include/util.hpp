
#ifndef CLEVEL_UTIL_HPP
#define CLEVEL_UTIL_HPP
#include <chrono>
#include <cstdarg>
#include <fmt/core.h>
#include <fmt/ostream.h>
#include <sched.h>
#include <string_view>
namespace clevel_ns {

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
}// namespace clevel_ns
#endif