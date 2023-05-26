#ifndef PMHB_ADAPTER_STEPH_HPP
#define PMHB_ADAPTER_STEPH_HPP

#include "bench.hpp"
#include "bench_interface.hpp"
#include "steph.hpp"


namespace pmhb_ns::adapter {


using steph_map_type = steph_ns::steph<varlen_kv>;

struct steph : public bench_interface<steph_map_type> {
    using map_type = steph_map_type;

    map_type *do_open(config const &cfg, size_t kv_uulo) override {
        auto path = cfg.working_dir / "steph";
        auto depth = 8ul;
        std::filesystem::remove_all(path);
        auto map = map_type::open(path, MAP_STRUCTURE_SIZE, depth, kv_uulo);
        return map;
    }

    double load_factor(map_type *map, size_t current_kv_num) override {
        auto ret = map->get_memory_usage();
        // fmt::print("{} th with size {}, lf = {}\n", current_kv_num, ret,
        //            (double) current_kv_num * 8 / ret);
        return (double) current_kv_num * 8 / ret;
        // return (double) current_kv_num * 8 / map->get_memory_usage();
    }

    map_type *do_recover(config const &cfg) override {
        auto path = cfg.working_dir / "steph";
        auto map = map_type::open(path);

        auto out_path = cfg.output_dir / "recover";
        auto f = fmt::output_file(out_path.c_str());
        f.print("{}\n", clock::now().time_since_epoch().count());
        f.close();
        map->recover();

        return map;
    }

    void do_close(map_type *map, config const &cfg) override {
        map_type::close(map);
    }

    void do_ycsb_insert(map_type *map, context *ctx,
                        pmhb_ns::ycsb::INSERT const &cmd, size_t off,
                        bool is_load = false) override {
#ifdef WRITE_KV
        map->insert(cmd.key(), std::string_view{cmd.value()}.substr(0, 32),
                    0ul);
#else
        map->insert(cmd.key(), std::string_view{cmd.value()}.substr(0, 32), off,
                    is_load);
#endif


        // auto ret = map->search("628478186066737");
        // if (ret == nullptr) [[unlikely]] {
        //     fmt::print("search 628478186066737 returned nullptr aftger {}",
        //                cmd.key());
        //     exit(1);
        // }
    }

    void do_ycsb_read(map_type *map, context *ctx,
                      pmhb_ns::ycsb::READ const &cmd) override {
        auto ret = map->search(cmd.key());
        // if (ret == nullptr) [[unlikely]] {
        //         fmt::print("search {} returned nullptr", cmd.key());
        //     }
    }

    void do_ycsb_update(map_type *map, context *ctx,
                        pmhb_ns::ycsb::UPDATE const &cmd,
                        size_t off = 0) override {
#ifdef WRITE_KV
        auto ret = map->update(
                cmd.key(), std::string_view{cmd.value()}.substr(0, 32), 0ul);
#else
        auto ret = map->update(
                cmd.key(), std::string_view{cmd.value()}.substr(0, 32), off);
#endif
        // if (ret == false) {
        //     fmt::print("Not found\n");
        //     exit(0);
        // }
    }

    void do_ycsb_delete(map_type *map, context *ctx,
                        pmhb_ns::ycsb::DELETE const &cmd) override {
        auto ret = map->Delete(cmd.key());
        // if (ret == false) {
        //     fmt::print("Delete failed\n");
        //     exit(1);
        // }
    }

    void do_ycsb_check(map_type *map, context *ctx,
                       pmhb_ns::ycsb::CHECK const &cmd) override {
        auto ret = map->search(cmd.key());
        // if (ret) {
        //     int diff = strcmp(cmd.value(), ret->value());
        //     if (diff) {
        //         fmt::print("Diff {} with key {}, expect {} but got {}\n", diff,
        //                    cmd.key(), cmd.value(), ret->value());
        //         exit(0);
        //     }
        // } else if (ret == nullptr) {
        //     fmt::print("Didn't find the key {}\n", cmd.key());
        //     exit(0);
        // }
    }
};


}// namespace pmhb_ns::adapter

#endif