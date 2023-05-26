#ifndef PMHB_ADAPTER_LEVEL_HPP
#define PMHB_ADAPTER_LEVEL_HPP
#include "bench_interface.hpp"
#include "level.hpp"
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/transaction.hpp>
namespace pmhb_ns::adapter {

using level_map_type = level_ns::LevelHashing<varlen_kv>;

struct level : public bench_interface<level_map_type> {
    using map_type = level_map_type;

    struct root_type {
        pmem::obj::persistent_ptr<map_type> map{};
    };

    map_type *do_open(config const &cfg, size_t kv_uulo) override {
        auto path = cfg.working_dir / "level";
        std::filesystem::remove_all(path);
        auto pool = pmem::obj::pool<root_type>::create(
                path, "level", MAP_STRUCTURE_SIZE, 0666);
        auto root = pool.root();
        auto ret = root.get();
        fmt::print("created the pool\n");
        /* To eliminate the page faults for the main pool */
        pre_fault(pool.handle(), MAP_STRUCTURE_SIZE);
        pmem::obj::transaction::run(pool, [&]() {
            root->map = pmem::obj::make_persistent<map_type>();
            // root->map->set_thread_num(cfg.thread_num);
        });
        // root->map->
        int init_level = 16;
        level_ns::initialize_level(pool.handle(), root->map.get(),
                                   (void *) &init_level);
        fmt::print("open succeed\n");
#ifndef WRITE_KV
        level_ns::c_ptr<varlen_kv::K_TYPE>::pool_uuid_lo = kv_uulo;
        level_ns::c_ptr<varlen_kv::V_TYPE>::pool_uuid_lo = kv_uulo;
        level_ns::c_ptr<varlen_kv>::pool_uuid_lo = kv_uulo;
        fmt::print("UULO is {:x}\n", kv_uulo);
#endif
        return root->map.get();
    }
    double load_factor(map_type *map, size_t current_kv_num) override {
        return (double) current_kv_num * 16 / map->get_memory_usage();
    }

    map_type *do_recover(config const &cfg) override {
        auto path = cfg.working_dir / "level";
        auto pool = pmem::obj::pool<root_type>::open(path, "level");
        auto root = pool.root();
        auto ret = root.get();
        fmt::print("opened the pool\n");

        auto out_path = cfg.output_dir / "recover";
        auto f = fmt::output_file(out_path.c_str());
        f.print("{}\n", clock::now().time_since_epoch().count());
        f.close();
        root->map->Recovery(pool.handle());


        return root->map.get();
    }

    void do_close(map_type *level, config const &cfg) override {
        level->~map_type();
    }

    void do_ycsb_insert(map_type *map, context *ctx, ycsb::INSERT const &cmd,
                        size_t off = 0, bool is_load = false) override {
#ifdef WRITE_KV
        level_ns::c_ptr<varlen_kv> p_kv(0ul);
#else
        level_ns::c_ptr<varlen_kv> p_kv(off);
#endif
        auto ret = map->Insert(cmd.key(), cmd.value(), p_kv);
    }
    void do_ycsb_read(map_type *map, context *ctx,
                      ycsb::READ const &cmd) override {
        auto ret = map->Get(cmd.key());
    }
    void do_ycsb_update(map_type *map, context *ctx, ycsb::UPDATE const &cmd,
                        size_t off = 0) override {
#ifdef WRITE_KV
        level_ns::c_ptr<varlen_kv> p_kv(0ul);
#else
        level_ns::c_ptr<varlen_kv> p_kv(off);
#endif
        auto ret = map->Update(cmd.key(), cmd.value(), p_kv);
        // if (ret == false) {
        //     fmt::print("update failed\n");
        //     exit(0);
        // }
    }
    void do_ycsb_delete(map_type *map, context *ctx,
                        ycsb::DELETE const &cmd) override {
        map->Delete(cmd.key());
    }
};

}// namespace pmhb_ns::adapter
#endif
