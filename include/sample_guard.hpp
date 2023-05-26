#ifndef PMHB_SAMPLE_GUARD_HPP
#define PMHB_SAMPLE_GUARD_HPP

#include "bench.hpp"
#include "context.hpp"
#include <thread>
using namespace std::chrono_literals;

namespace pmhb_ns {

template<typename map_type, typename OP>
struct [[nodiscard]] sample_guard {
    explicit sample_guard(size_t _number = 0) : number{_number} {
        auto that = bench<map_type>::g_bench;
        if (that == nullptr) [[unlikely]] {
            // fmt::print("sample target not found!");
            return;
        }
        auto id = std::this_thread::get_id();
        auto pos = that->profiler->ctxs.find(id);
        if (pos == that->profiler->ctxs.end()) [[unlikely]] {
            auto [newpos, ok] = that->profiler->ctxs.insert(
                    {id, std::make_shared<context>(4'000'000)});
            if (!ok) {
                fmt::print("context creation failure");
            } else {
                fmt::print("context created");
            }
            pos = newpos;
        }
        if (number) {
            auto tmp = operation{OP{}};
            if (tmp.index() == operation{WRITE_COUNT{}}.index()) {
                /* WRITE_COUNT */
                pos->second->add_write((number + 255) & (~255ul));
            } else {
                std::get<RESIZE_ITEM_NUMBER>(tmp).elapsed_time =
                        std::chrono::nanoseconds(number);
                pos->second->samples.push_back(std::move(tmp));
            }
        } else {
            /* SEARCH, INSERT, REHASH, DOUBLE, UPDATE, DELETE */
            pos->second->start(OP{});
        }
    }

    ~sample_guard() {
        /* WRITE_COUNT */
        if (number) { return; }
        /* SEARCH, INSERT, REHASH, DOUBLE, UPDATE, DELETE */
        auto that = bench<map_type>::g_bench;
        if (that == nullptr) [[unlikely]] {
            // fmt::print("cannot find previous existed sample target!");
            return;
        }
        auto ctx = that->profiler->ctxs[std::this_thread::get_id()];
        ctx->finish(OP{});
    }

    sample_guard(sample_guard const &) = delete;
    sample_guard &operator=(sample_guard const &) = delete;
    sample_guard(sample_guard &&) = delete;
    sample_guard &operator=(sample_guard &&) = delete;

private:
    size_t number{0};
};


}// namespace pmhb_ns


#endif//PMHB_SAMPLE_GUARD_HPP
