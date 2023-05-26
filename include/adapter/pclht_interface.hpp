#ifndef PMHB_ADAPTER_PCLHT_HPP
#define PMHB_ADAPTER_PCLHT_HPP
#include "bench_interface.hpp"
#include "pclht.hpp"
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/transaction.hpp>
namespace pmhb_ns::adapter {

using clht_map_type = pclht_ns::clht<std::array<char, STRING_LENGTH>,
                                     std::array<char, STRING_LENGTH>>;

struct clht : public bench_interface<clht_map_type> {
    using map_type = clht_map_type;
    using key_type = map_type::key_type;
    using mapped_type = map_type::mapped_type;
    using value_type = map_type::value_type;

    struct root_type {
        pmem::obj::persistent_ptr<map_type> map{};
    };

    map_type *do_open(config const &cfg, size_t kv_uulo) override {
        auto path = cfg.working_dir / "clht";
        std::filesystem::remove_all(path);
        auto pool = pmem::obj::pool<root_type>::create(
                path, "clht", MAP_STRUCTURE_SIZE, 0666);
        auto root = pool.root();
        auto ret = root.get();
        {
            /* To eliminate the page faults for the main pool */
            pmhb_ns::pre_fault(pool.handle(), MAP_STRUCTURE_SIZE);
        }
        pmem::obj::transaction::run(pool, [&]() {
            root->map = pmem::obj::make_persistent<map_type>(1024ul << 8);
            // root->map->set_thread_num(cfg.thread_num);
        });
#ifndef WRITE_KV
        root->map->kv_pool_uuid = kv_uulo;
#endif
        return root->map.get();
    }

    map_type *do_recover(config const &cfg) override {
        auto path = cfg.working_dir / "clht";
        auto pool = pmem::obj::pool<root_type>::open(path, "clht");
        auto root = pool.root();
        auto ret = root.get();

        auto out_path = cfg.output_dir / "recover";
        auto f = fmt::output_file(out_path.c_str());
        f.print("{}\n", clock::now().time_since_epoch().count());
        f.close();
        // root->map->set_thread_num(cfg.thread_num);


        return root->map.get();
    }

    double load_factor(map_type *clht, size_t current_kv_num) override {
        return (double) current_kv_num * 8 / clht->get_memory_usage();
    }

    void do_close(map_type *clht, config const &cfg) override {
        clht->~map_type();
    }

    void do_ycsb_insert(map_type *map, context *ctx, ycsb::INSERT const &cmd,
                        size_t pkv, bool is_load = false) override {
        static bool start = false;
        auto k = key_type{};
        strncpy(k.begin(), cmd.key(), STRING_LENGTH);
        auto v = mapped_type{};
        strncpy(v.begin(), cmd.value(), STRING_LENGTH);
#ifndef WRITE_KV
        map->put({k, v}, ctx->tid, pkv + sizeof(varlen_kv));
#else
        map->put({k, v}, ctx->tid, 0);
#endif
        // if (strncmp(k.begin(), "909749830972671", STRING_LENGTH) == 0) {
        //     start = true;
        // }
        // if (start) {
        //     auto target_k = key_type{};
        //     strncpy(target_k.begin(), "909749830972671", STRING_LENGTH);
        //     auto ret = map->get(target_k);
        //     if (ret.found == false) {
        //         fmt::print("Not found after insert {} until pkv = {}\n",
        //                    k.begin(), pkv);
        //         exit(0);
        //     }
        // }
    }
    void do_ycsb_read(map_type *map, context *ctx,
                      ycsb::READ const &cmd) override {
        auto k = key_type{};
        strncpy(k.begin(), cmd.key(), STRING_LENGTH);
        map->get(k);
    }
    void do_ycsb_update(map_type *map, context *ctx, ycsb::UPDATE const &cmd,
                        size_t pkv) override {
        auto k = key_type{};
        strncpy(k.begin(), cmd.key(), STRING_LENGTH);
        auto v = mapped_type{};
        strncpy(v.begin(), cmd.value(), STRING_LENGTH);


        auto ret = map->update(k, ctx->tid, pkv + sizeof(varlen_kv));

        // if (ret.found == false) {
        //     fmt::print("Key not found for {}\n", k.begin());

        //     auto search_ret = map->get(k);
        //     if (search_ret.found) {
        //         fmt::print("Key found\n");
        //     } else {
        //         fmt::print("search not found\n");
        //     }
        //     exit(0);
        // }
    }
    void do_ycsb_delete(map_type *map, context *ctx,
                        ycsb::DELETE const &cmd) override {
        auto k = key_type{};
        strncpy(k.begin(), cmd.key(), STRING_LENGTH);
        map->erase(k);
    }
    void do_ycsb_check(map_type *map, context *ctx, ycsb::CHECK const &cmd) {
        auto k = key_type{};
        strncpy(k.begin(), cmd.key(), STRING_LENGTH);
        auto ret = map->get(k);
        if (ret.found == false) {
            fmt::print("no result for key {}\n", cmd.key());
            exit(0);
        }
        // if (strcmp(cmd.value(), ret.) != 0) {
        //     fmt::print("not equal at key {}\n", cmd.key());
        //     exit(0);
        // }
    }
};

}// namespace pmhb_ns::adapter

#endif//PMHB_ADAPTER_PCLHT_HPP
