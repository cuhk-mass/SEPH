#ifndef PMHB_CONTEXT_HPP
#define PMHB_CONTEXT_HPP

#include "utils.hpp"
#include <atomic>
#include <list>
#include <variant>
#include <vector>

namespace pmhb_ns {

struct time_slice {
    time_point start_time{clock::now()};// 8B
    duration elapsed_time{};            // 8B
};

struct SEARCH : time_slice {};
struct INSERT : time_slice {};
struct REHASH : time_slice {};
struct DOUBLE : time_slice {};
struct UPDATE : time_slice {};
struct DELETE : time_slice {};
struct WRITE_COUNT : time_slice {};
struct RESIZE_ITEM_NUMBER : time_slice {};

using operation = std::variant<INSERT, SEARCH, REHASH, DOUBLE, UPDATE, DELETE,
                               RESIZE_ITEM_NUMBER, WRITE_COUNT>;


struct pending_list {
    std::array<std::pair<bool, operation>, 10> pending;

    void push(operation op_) {
        for (auto &[valid, op] : pending) {
            if (valid == false) {
                valid = true;
                op = op_;
                return;
            }
        }
    }

    operation pop(size_t idx) {
        for (auto &[valid, op] : pending) {
            if (valid && op.index() == idx) {
                valid = false;
                return op;
            }
        }
        fmt::print("Not found\n");
        exit(0);
    }
};

// per thread structure
struct context {
    size_t tid{0};
    size_t write_count{0};
    std::vector<operation> samples{};
    // std::list<operation> pending{};
    pending_list pending;

    /* reserved for 100 million */
    context(size_t reserved_num = 3'000'000) {
        samples.reserve(reserved_num);
        // fmt::print("capacity is {}\n", samples.capacity());
        pre_fault(samples.data(), sizeof(operation) * samples.capacity(), 4096);
    }

    void start(operation &&op) { pending.push(std::move(op)); }
    // void start(operation &&op) { pending.push_front(std::move(op)); }

    void finish(operation &&op) {
        // auto pos = std::find_if(
        //         std::begin(pending), std::end(pending),
        //         [&](auto const &elem) { return elem.index() == op.index(); });
        // auto match = operation{};
        // std::swap(*pos, match);
        // pending.erase(pos);
        auto match = pending.pop(op.index());
        std::visit(
                overload{[&](SEARCH &m) {
                             m.elapsed_time = std::get<SEARCH>(op).start_time -
                                              m.start_time;
                         },
                         [&](INSERT &m) {
                             m.elapsed_time = std::get<INSERT>(op).start_time -
                                              m.start_time;
                         },
                         [&](REHASH &m) {
                             m.elapsed_time = std::get<REHASH>(op).start_time -
                                              m.start_time;
                         },
                         [&](DOUBLE &m) {
                             m.elapsed_time = std::get<DOUBLE>(op).start_time -
                                              m.start_time;
                         },
                         [&](UPDATE &m) {
                             m.elapsed_time = std::get<UPDATE>(op).start_time -
                                              m.start_time;
                         },
                         [&](DELETE &m) {
                             m.elapsed_time = std::get<DELETE>(op).start_time -
                                              m.start_time;
                         },
                         [&](RESIZE_ITEM_NUMBER &) {}, [&](WRITE_COUNT &) {}},

                match);
        samples.push_back(std::move(match));
    }
    void add_write(size_t writes) { write_count += writes; }
};

}// namespace pmhb_ns

#endif//PMHB_CONTEXT_HPP
