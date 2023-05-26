#ifndef PMHB_ADAPTER_CLEVEL_HPP
#define PMHB_ADAPTER_CLEVEL_HPP
#include "bench_interface.hpp"
#include "clevel.hpp"
#include "utils.hpp"
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/transaction.hpp>
namespace pmhb_ns::adapter {

using clevel_map_type = clevel_ns::clevel_hash<std::array<char, STRING_LENGTH>,
                                               std::array<char, STRING_LENGTH>>;

struct clevel : public bench_interface<clevel_map_type> {
    using map_type = clevel_map_type;
    using key_type = map_type::key_type;
    using mapped_type = map_type::mapped_type;
    using value_type = map_type::value_type;

    struct root_type {
        pmem::obj::persistent_ptr<map_type> map{};
    };

    map_type *do_open(config const &cfg, size_t kv_uulo) override {
        auto path = cfg.working_dir / "clevel";
        std::filesystem::remove_all(path);
        auto pool = pmem::obj::pool<root_type>::create(
                path, "clevel", MAP_STRUCTURE_SIZE, 0666);
        auto root = pool.root();
        auto ret = root.get();
        {
            /* To eliminate the page faults for the main pool */
            pmhb_ns::pre_fault(pool.handle(), MAP_STRUCTURE_SIZE);
        }
        pmem::obj::transaction::run(pool, [&]() {
            root->map = pmem::obj::make_persistent<map_type>();
            root->map->set_thread_num(cfg.thread_num);
        });
#ifndef WRITE_KV
        root->map->kv_pool_uuid = kv_uulo;
#endif
        return root->map.get();
    }

    map_type *do_recover(config const &cfg) override {
        auto path = cfg.working_dir / "clevel";
        auto pool = pmem::obj::pool<root_type>::open(path, "clevel");
        auto root = pool.root();
        auto ret = root.get();

        auto out_path = cfg.output_dir / "recover";
        auto f = fmt::output_file(out_path.c_str());
        f.print("{}\n", clock::now().time_since_epoch().count());
        f.close();
        root->map->set_thread_num(cfg.thread_num);


        return root->map.get();
    }

    double load_factor(map_type *clevel, size_t current_kv_num) override {
        return (double) current_kv_num * 8 / clevel->get_memory_usage();
    }

    void do_close(map_type *clevel, config const &cfg) override {
        clevel->~map_type();
    }

    void do_ycsb_insert(map_type *map, context *ctx, ycsb::INSERT const &cmd,
                        size_t pkv, bool is_load = false) override {
        auto k = key_type{};
        strncpy(k.begin(), cmd.key(), STRING_LENGTH);
        auto v = mapped_type{};
        strncpy(v.begin(), cmd.value(), STRING_LENGTH);
#ifndef WRITE_KV
        map->insert({k, v}, ctx->tid, 0, pkv + sizeof(varlen_kv));
#else
        map->insert({k, v}, ctx->tid, 0, 0);
#endif
    }
    void do_ycsb_read(map_type *map, context *ctx,
                      ycsb::READ const &cmd) override {
        auto k = key_type{};
        strncpy(k.begin(), cmd.key(), STRING_LENGTH);
        map->search(k);
    }
    void do_ycsb_update(map_type *map, context *ctx, ycsb::UPDATE const &cmd,
                        size_t pkv = 0) override {
        auto k = key_type{};
        strncpy(k.begin(), cmd.key(), STRING_LENGTH);
        auto v = mapped_type{};
        strncpy(v.begin(), cmd.value(), STRING_LENGTH);
#ifndef WRITE_KV
        auto ret = map->update({k, v}, ctx->tid, pkv + sizeof(varlen_kv));
#else
        auto ret = map->update({k, v}, ctx->tid, 0);
#endif
        // if (ret.found == false) {
        //     fmt::print("update failed\n");
        //     exit(0);
        // }
    }
    void do_ycsb_delete(map_type *map, context *ctx,
                        ycsb::DELETE const &cmd) override {
        auto k = key_type{};
        strncpy(k.begin(), cmd.key(), STRING_LENGTH);
        map->erase(k, ctx->tid);
    }
    void do_ycsb_check(map_type *map, context *ctx, ycsb::CHECK const &cmd) {
        auto k = key_type{};
        strncpy(k.begin(), cmd.key(), STRING_LENGTH);
        // fmt::print("check {}\n", k.data());
        auto ret = map->search(k);
        if (!ret.found) {
            fmt::print("not found\n");
            exit(0);
        }
    }
};

}// namespace pmhb_ns::adapter

#endif//PMHB_ADAPTER_CLEVEL_HPP
