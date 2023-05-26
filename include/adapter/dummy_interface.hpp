#ifndef PMHB_ADAPTER_DUMMY_HPP
#define PMHB_ADAPTER_DUMMY_HPP
#include "bench.hpp"

#include "dummy.hpp"
#include "utils.hpp"

#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/transaction.hpp>

#pragma GCC diagnostic ignored "-Wunused-variable"

namespace pmhb_ns::adapter {

using dummy_map_type = dummy_ns::dummy<varlen_kv>;

struct dummy : public bench_interface<dummy_map_type> {
    using map_type = dummy_map_type;

    map_type *do_open(config const &cfg, size_t kv_uulo) override {
        auto map = dummy_ns::dummy<varlen_kv>::Open();
        return map;
    }

    map_type *do_recover(config const &cfg) override {
        auto map = dummy_ns::dummy<varlen_kv>::Recover();
        return map;
    }

    void do_close(map_type *map, config const &cfg) override {
        map->~map_type();
    }

    void do_ycsb_insert(map_type *map, context *ctx, ycsb::INSERT const &cmd,
                        size_t off, bool is_load = false) override {
        // fmt::print("{} {}\n", cmd.key(), cmd.value());
        auto ret = map->Insert(cmd.key(), cmd.value());
    }
    void do_ycsb_read(map_type *map, context *ctx,
                      ycsb::READ const &cmd) override {
        auto ret = map->Search(cmd.key());
    }
    void do_ycsb_update(map_type *map, context *ctx,
                        ycsb::UPDATE const &cmd) override {
        auto ret = map->Update(cmd.key(), cmd.value());
    }
    void do_ycsb_delete(map_type *map, context *ctx,
                        ycsb::DELETE const &cmd) override {
        map->Delete(cmd.key());
    }

    double load_factor(map_type *map, size_t current_kv_num) override {
        return map->Load_factor();
    }
};

}// namespace pmhb_ns::adapter
#endif
