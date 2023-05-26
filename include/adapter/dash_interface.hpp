#ifndef PMHB_ADAPTER_DASH_HPP
#define PMHB_ADAPTER_DASH_HPP

#include "bench_interface.hpp"
#include "dash.hpp"


namespace pmhb_ns::adapter {

// inline constexpr auto STRING_LENGTH = 32ul;


struct dash : public bench_interface<dash_ns::Finger_EH<varlen_kv>> {
    using map_type = dash_ns::Finger_EH<varlen_kv>;

    map_type *do_open(config const &cfg, size_t kv_uulo) override {
        auto path = cfg.working_dir / "dash";
        std::filesystem::remove_all(path);
        dash_ns::Allocator::Initialize(path.c_str(),
                                       pmhb_ns::MAP_STRUCTURE_SIZE);
        fmt::print("inited\n");
        auto map = reinterpret_cast<map_type *>(
                dash_ns::Allocator::GetRoot(sizeof(map_type)));
        new (map) map_type(1ul << 8, dash_ns::Allocator::Get()->pm_pool_);
#ifndef WRITE_KV
        dash_ns::c_ptr<varlen_kv::K_TYPE>::pool_uuid_lo = kv_uulo;
        dash_ns::c_ptr<varlen_kv::V_TYPE>::pool_uuid_lo = kv_uulo;
        dash_ns::c_ptr<varlen_kv>::pool_uuid_lo = kv_uulo;
        fmt::print("UULO is {:x}\n", kv_uulo);
#endif
        return map;
    }

    map_type *do_recover(config const &cfg) override {
        auto path = cfg.working_dir / "dash";
        dash_ns::Allocator::Initialize(path.c_str(),
                                       pmhb_ns::MAP_STRUCTURE_SIZE);
        auto map = reinterpret_cast<map_type *>(
                dash_ns::Allocator::GetRoot(sizeof(map_type)));

        auto out_path = cfg.output_dir / "recover";
        auto f = fmt::output_file(out_path.c_str());
        f.print("{}\n", clock::now().time_since_epoch().count());
        f.close();
        map->Recovery(dash_ns::Allocator::instance_->pm_pool_);


        return map;
    }
    double load_factor(map_type *map, size_t current_kv_num) override {
        return (double) current_kv_num * 16 / map->get_memory_usage();
    }

    void do_close(map_type *map, config const &cfg) override {
        map->~map_type();
        dash_ns::Allocator::Close_pool();
    }

    void do_ycsb_insert(map_type *map, context *ctx,
                        pmhb_ns::ycsb::INSERT const &cmd, size_t pkv,
                        bool is_load = false) override {
#ifndef WRITE_KV
        auto k_ptr = dash_ns::c_ptr<varlen_kv::K_TYPE>(pkv + sizeof(varlen_kv));
        auto v_ptr = dash_ns::c_ptr<varlen_kv::V_TYPE>(
                pkv + sizeof(varlen_kv) + sizeof(varlen_kv::K_TYPE));
#else
        auto k_ptr = dash_ns::c_ptr<varlen_kv::K_TYPE>(0ul);
        auto v_ptr = dash_ns::c_ptr<varlen_kv::V_TYPE>(0ul);
#endif
        auto ret = map->Insert(cmd.key(), cmd.value(), k_ptr, v_ptr);
        // if (ret < 0) [[unlikely]] { fmt::print("insert failed {} {}", ret, cmd.key()); }
        // auto val = map->Get(cmd.key());
        // if (val == nullptr) [[unlikely]] {
        //     fmt::print("insert with nullptr {}", cmd.key());
        //     exit(0);
        // } else if (val != cmd.value()) {
        //     fmt::print("val is different get {}, cmd is {} - {}", val, cmd.key(), cmd.value());
        //     exit(0);
        // } else {
        //     // fmt::print("insert success with cmd {} - {}", cmd.key(), cmd.value());
        // }
    }
    void do_ycsb_read(map_type *map, context *ctx,
                      pmhb_ns::ycsb::READ const &cmd) override {
        auto ret = map->Get(cmd.key());
        // if (ret == nullptr) [[unlikely]] {
        //     fmt::print("search failed {}", cmd.key());
        //     // exit(0);
        // }
    }
    void do_ycsb_update(map_type *map, context *ctx,
                        pmhb_ns::ycsb::UPDATE const &cmd, size_t pkv) override {
        // static size_t counter = 0;
#ifndef WRITE_KV
        auto k_ptr = dash_ns::c_ptr<varlen_kv::K_TYPE>(pkv + sizeof(varlen_kv));
        auto v_ptr = dash_ns::c_ptr<varlen_kv::V_TYPE>(
                pkv + sizeof(varlen_kv) + sizeof(varlen_kv::K_TYPE));
#else
        auto k_ptr = dash_ns::c_ptr<varlen_kv::K_TYPE>(0ul);
        auto v_ptr = dash_ns::c_ptr<varlen_kv::V_TYPE>(0ul);
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
                        pmhb_ns::ycsb::DELETE const &cmd) override {
        map->Delete(cmd.key());
    }
    void do_ycsb_check(map_type *map, context *ctx,
                       pmhb_ns::ycsb::CHECK const &cmd) override {
        auto ret = map->Get(cmd.key());
        // if (ret) {
        //     int diff = strcmp(cmd.value(), ret);
        //     if (diff) {
        //         fmt::print("Diff {} with key {}, expect {} but got {}\n", diff,
        //                    cmd.key(), cmd.value(), ret);
        //         exit(0);
        //     }
        // } else if (ret == nullptr) {
        //     fmt::print("Didn't find the key {}\n", cmd.key());
        //     exit(0);
        // }
    }
};

}// namespace pmhb_ns::adapter

#endif//PMHB_ADAPTER_DASH_HPP
