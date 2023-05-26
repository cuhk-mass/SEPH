#ifndef PMHB_ADAPTER_CCEH_HPP
#define PMHB_ADAPTER_CCEH_HPP
#include "bench_interface.hpp"
#include "cceh.hpp"

#include <fmt/core.h>
#include <fmt/ostream.h>

#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/transaction.hpp>
namespace pmhb_ns::adapter {

using cceh_map_type = cceh_ns::CCEH<varlen_kv>;

struct cceh : public bench_interface<cceh_map_type> {
    using map_type = cceh_map_type;

    map_type *do_open(config const &cfg, size_t kv_uulo) override {
        auto path = cfg.working_dir / "cceh";
        std::filesystem::remove_all(path);
        // auto map = map_type::open(path, pmhb_ns::MAP_STRUCTURE_SIZE, 1ul << 8);
        // return map;


        cceh_ns::Allocator::Initialize(path.c_str(),
                                       pmhb_ns::MAP_STRUCTURE_SIZE);
        auto map = reinterpret_cast<map_type *>(
                cceh_ns::Allocator::GetRoot(sizeof(map_type)));
        new (map) map_type(1ul << 8, cceh_ns::Allocator::Get()->pm_pool_);
#ifndef WRITE_KV
        cceh_ns::c_ptr<varlen_kv::K_TYPE>::pool_uuid_lo = kv_uulo;
        cceh_ns::c_ptr<varlen_kv::V_TYPE>::pool_uuid_lo = kv_uulo;
        cceh_ns::c_ptr<varlen_kv>::pool_uuid_lo = kv_uulo;
        fmt::print("UULO is {:x}\n", kv_uulo);
#endif
        return map;
    }

    map_type *do_recover(config const &cfg) override {
        auto path = cfg.working_dir / "cceh";
        cceh_ns::Allocator::Initialize(path.c_str(),
                                       pmhb_ns::MAP_STRUCTURE_SIZE);
        auto map = reinterpret_cast<map_type *>(
                cceh_ns::Allocator::GetRoot(sizeof(map_type)));

        auto out_path = cfg.output_dir / "recover";
        auto f = fmt::output_file(out_path.c_str());
        f.print("{}\n", clock::now().time_since_epoch().count());
        f.close();
        map->Recovery(cceh_ns::Allocator::instance_->pm_pool_);

        return map;
    }

    double load_factor(map_type *map, size_t current_kv_num) override {
        return (double) current_kv_num * 16 / map->get_memory_usage();
    }

    void do_close(map_type *cceh, config const &cfg) override {
        cceh->~map_type();
    }

    void do_ycsb_insert(map_type *map, context *ctx, ycsb::INSERT const &cmd,
                        size_t pkv, bool is_load = false) override {
#ifndef WRITE_KV
        auto k_ptr = cceh_ns::c_ptr<varlen_kv::K_TYPE>(pkv + sizeof(varlen_kv));
        auto v_ptr = cceh_ns::c_ptr<varlen_kv::V_TYPE>(
                pkv + sizeof(varlen_kv) + sizeof(varlen_kv::K_TYPE));
#else
        auto k_ptr = cceh_ns::c_ptr<varlen_kv::K_TYPE>(0ul);
        auto v_ptr = cceh_ns::c_ptr<varlen_kv::V_TYPE>(0ul);
#endif
        auto ret = map->Insert(cmd.key(), cmd.value(), k_ptr, v_ptr);
    }
    void do_ycsb_read(map_type *map, context *ctx,
                      ycsb::READ const &cmd) override {
        auto ret = map->Get(cmd.key());
    }
    void do_ycsb_update(map_type *map, context *ctx, ycsb::UPDATE const &cmd,
                        size_t pkv) override {
#ifndef WRITE_KV
        auto k_ptr = cceh_ns::c_ptr<varlen_kv::K_TYPE>(pkv + sizeof(varlen_kv));
        auto v_ptr = cceh_ns::c_ptr<varlen_kv::V_TYPE>(
                pkv + sizeof(varlen_kv) + sizeof(varlen_kv::K_TYPE));
#else
        auto k_ptr = cceh_ns::c_ptr<varlen_kv::K_TYPE>(0ul);
        auto v_ptr = cceh_ns::c_ptr<varlen_kv::V_TYPE>(0ul);
#endif
        auto ret = map->Update(cmd.key(), cmd.value(), k_ptr, v_ptr);
        // if (ret == false) [[unlikely]] {
        //     fmt::print("update failed\n");
        //     auto search_ret = map->Get(cmd.key());
        //     if (search_ret) {
        //         fmt::print("search found the key\n");
        //     } else {
        //         fmt::print("search not found\n");
        //     }
        //     exit(0);
        // }
    }
    void do_ycsb_delete(map_type *map, context *ctx,
                        ycsb::DELETE const &cmd) override {
        map->Delete(cmd.key());
    }
    void do_ycsb_check(map_type *map, context *ctx, ycsb::CHECK const &cmd) {
        auto ret = map->Get(cmd.key());
        if (ret == nullptr) {
            fmt::print("no result for key {}\n", cmd.key());
            exit(0);
        }
        if (strcmp(cmd.value(), ret) != 0) {
            fmt::print("not equal at key {}\n", cmd.key());
            exit(0);
        }
    }
};

}// namespace pmhb_ns::adapter
#endif
