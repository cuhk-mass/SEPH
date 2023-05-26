#ifndef PMHB_BENCH_INTERFACE_HPP
#define PMHB_BENCH_INTERFACE_HPP

#include "config.hpp"
#include "context.hpp"
#include "ycsb.hpp"
#pragma GCC diagnostic ignored "-Wunused-parameter"
namespace pmhb_ns {
// cannot make it static here, because sample_guard needs a single entry point
// template<typename interface_type, typename map_type>
// concept bench_interface = requires(map_type *map, config const &cfg,
//                                    context *ctx, ycsb::command const &cmd) {
//     { interface_type::do_open(cfg) }
//     ->std::same_as<map_type *>;
//     {interface_type::do_ycsb_command(map, cmd, ctx)};
//     {interface_type::do_close(map, cfg)};
// };

template<typename map_type>
struct bench_interface {
    virtual map_type *do_open(config const &, size_t kv_uulo = 0) {
        fmt::print("no interface provided!");
        return nullptr;
    }
    virtual map_type *do_recover(config const &cfg) {
        fmt::print("no interface provided!");
        return nullptr;
    }
    virtual void do_close(map_type *, config const &) {
        fmt::print("no interface provided!");
    }
    virtual void do_ycsb_insert(map_type *, context *, ycsb::INSERT const &,
                                size_t off = 0, bool is_load = false) {
        fmt::print("no interface provided!");
    }
    virtual void do_ycsb_read(map_type *, context *, ycsb::READ const &) {
        fmt::print("no interface provided!");
    }
    virtual void do_ycsb_update(map_type *, context *, ycsb::UPDATE const &,
                                size_t off = 0) {
        fmt::print("no interface provided!");
    }
    virtual void do_ycsb_delete(map_type *, context *, ycsb::DELETE const &) {
        fmt::print("no interface provided!");
    }
    virtual void do_ycsb_check(map_type *, context *, ycsb::CHECK const &) {
        fmt::print("no (check) interface provided!\n");
    }
    virtual void do_ycsb_command(map_type *map, context *ctx,
                                 ycsb::command const &cmd, size_t off = 0,
                                 bool is_load = false) {
        std::visit(overload{
                           [&](ycsb::INSERT const &cmd) {
                               do_ycsb_insert(map, ctx, cmd, off, is_load);
                           },
                           [&](ycsb::READ const &cmd) {
                               do_ycsb_read(map, ctx, cmd);
                           },
                           [&](ycsb::UPDATE const &cmd) {
                               do_ycsb_update(map, ctx, cmd, off);
                           },
                           [&](ycsb::DELETE const &cmd) {
                               do_ycsb_delete(map, ctx, cmd);
                           },
                           [&](ycsb::CHECK const &cmd) {
                               do_ycsb_check(map, ctx, cmd);
                           },
                           [&](ycsb::NONE const &) {},
                   },
                   cmd);
    }
    virtual double load_factor(map_type *map, size_t current_kv_num) {
        fmt::print("no interface provided!");
        return 0.0;
    }


    virtual ~bench_interface() = default;
};

}// namespace pmhb_ns

#endif// PMHB_BENCH_INTERFACE_HPP