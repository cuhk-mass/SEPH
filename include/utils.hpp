#ifndef PMHB_UTILS_HPP
#define PMHB_UTILS_HPP

#include <algorithm>
#include <array>
#include <chrono>
#include <filesystem>
#include <fmt/chrono.h>
#include <fmt/core.h>
#include <fmt/os.h>
#include <fmt/ranges.h>
#include <map>
#include <memory>
#include <sched.h>
#include <string>
#include <string_view>
#include <thread>
#include <tuple>

namespace pmhb_ns {

using usz = std::size_t;
using duration = std::chrono::nanoseconds;
using clock = std::chrono::high_resolution_clock;
using time_point = std::chrono::time_point<clock, duration>;


inline bool cpubind(int idx) {
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(idx, &set);
    int err = pthread_setaffinity_np(pthread_self(), sizeof(set), &set);
    if (err) {
        fmt::print("thread {} failed in pthread_setaffinity_np with err {}",
                   pthread_self(), err);
    }
    // fmt::print("thread {} running on core {}", pthread_self(), sched_getcpu());
    return !err;
}

/* To test the time consumming of every part*/
template<typename Callable, typename... Args>
std::tuple<std::invoke_result_t<Callable, Args...>, duration>
time(Callable &&f, Args &&...args) {
    auto begin = clock::now();
    auto ret = f(std::forward<Args>(args)...);
    auto end = clock::now();
    return {ret, end - begin};
}

template<class... Ts>
struct overload : Ts... {
    using Ts::operator()...;
};
template<class... Ts>
overload(Ts...) -> overload<Ts...>;

void time_log(const std::string_view info) {
    fmt::print("[{:%H:%M:%S}] {}\n", fmt::localtime(std::time(nullptr)),
               info.data());
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

void debug_point(int i) { fmt::print("[debug] {}\n", i); }


}// namespace pmhb_ns

namespace std {
template<size_t N>
struct hash<array<char, N>> {
    size_t operator()(array<char, N> const &arr) {
        return hash<string_view>{}(string_view{arr.data(), arr.size()});
    }
};

}// namespace std


#endif//PMHB_UTILS_HPP
