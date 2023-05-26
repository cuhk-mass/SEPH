#ifndef PMHB_YCSB_HPP
#define PMHB_YCSB_HPP

#include "config.hpp"
#include "varlen_kv.hpp"
#include <atomic>
#include <cctype>
#include <charconv>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <libpmem.h>
#include <libpmemobj++/make_persistent_array_atomic.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj.h>
#include <random>
#include <string_view>
#include <thread>
#include <variant>
#include <vector>

namespace pmhb_ns {
constexpr size_t CMD_SEGMENT_CAPACITY = 256ul << 20;

constexpr size_t CMD_SEGMENT_NUM = 64;

struct ycsb {
    /* Types */
    struct INSERT : varlen_kv {
        INSERT(std::string_view k, std::string_view v)
            : varlen_kv(STRING_LENGTH, k, STRING_LENGTH, v) {}

    private:
        char padding[2 * STRING_LENGTH];
    };
    struct READ : varlen_kv {
        READ(std::string_view k) : varlen_kv(STRING_LENGTH, k) {}
        // READ(INSERT cmd) : varlen_kv(STRING_LENGTH, cmd.key()) {}
        // READ() : varlen_kv(STRING_LENGTH, "") {}

    private:
        char padding[STRING_LENGTH];
    };
    struct UPDATE : varlen_kv {
        UPDATE(std::string_view k, std::string_view v)
            : varlen_kv(STRING_LENGTH, k, STRING_LENGTH, v) {}

    private:
        char padding[2 * STRING_LENGTH];
    };
    struct DELETE : varlen_kv {
        DELETE(std::string_view k) : varlen_kv(STRING_LENGTH, k) {}

    private:
        char padding[STRING_LENGTH];
    };

    struct CHECK : varlen_kv {
        CHECK() : varlen_kv(STRING_LENGTH, "", STRING_LENGTH, "") {}

        CHECK(std::string_view k, std::string_view v)
            : varlen_kv(STRING_LENGTH, k, STRING_LENGTH, v) {}

        CHECK(INSERT cmd)
            : varlen_kv(STRING_LENGTH, cmd.key(), STRING_LENGTH, cmd.value()) {}

    private:
        char padding[2 * STRING_LENGTH];
    };

    struct NONE : varlen_kv {};

    // C++ version of Rust's enum
    using command = std::variant<INSERT, READ, UPDATE, DELETE, CHECK, NONE>;


    struct root_type {
        size_t load_cmd_num = 0;
        size_t run_cmd_num = 0;
        PMEMoid command_segments[CMD_SEGMENT_NUM];

        /* Function */

        command *get_load_cmd(size_t idx) {
            size_t seg_idx = idx / CMD_SEGMENT_CAPACITY;
            size_t slot_idx = idx % CMD_SEGMENT_CAPACITY;
            return (command *) (pmemobj_direct(command_segments[seg_idx])) +
                   slot_idx;
        }

        command *get_run_cmd(size_t idx) {
            idx += load_cmd_num;
            size_t seg_idx = idx / CMD_SEGMENT_CAPACITY;
            size_t slot_idx = idx % CMD_SEGMENT_CAPACITY;
            return (command *) (pmemobj_direct(command_segments[seg_idx])) +
                   slot_idx;
        }
    };

    /* Data */
    const config cfg;

    pmem::obj::pool<root_type> pop;
    size_t uulo = 0;
    root_type *proot;
    // command *load_cmds, *run_cmds;


    /* Interfaces */
    ycsb(const config &_cfg) : cfg(_cfg) {
        pop = pop.open(cfg.pm_ycsb.c_str(), "KV");
        proot = pop.root().get();
        // load_cmds = ((command *) pmemobj_direct(proot->command_segments));
        // run_cmds = load_cmds + proot->load_cmd_num;
        if (proot->load_cmd_num < cfg.load_num ||
            proot->run_cmd_num < cfg.run_num) {
            fmt::print("Expected load {} KV but provided {}\n", cfg.load_num,
                       proot->load_cmd_num);
            fmt::print("Expected run {} KV but provided {}\n", cfg.run_num,
                       proot->run_cmd_num);
            exit(1);
        }
        uulo = proot->command_segments[0].pool_uuid_lo;
        fmt::print("load records {}, run records {}\n", proot->load_cmd_num,
                   proot->run_cmd_num);
        // pre_fault(load_cmds, proot->load_cmd_num * sizeof(command));
        // pre_fault(run_cmds, proot->run_cmd_num * sizeof(command));
        time_log("YCSB trace has been loaded");
    }
    ~ycsb() { pop.close(); }


    /* Helper function */
    // size_t load_offset(size_t round_i, size_t th_i) {
    //     size_t i = round_i * cfg.thread_num + th_i;
    //     return (uintptr_t) (load_cmds + i) - (uintptr_t) pop.handle();
    // }

    // size_t run_offset(size_t round_i, size_t th_i) {
    //     size_t i = round_i * cfg.thread_num + th_i;
    //     return (uintptr_t) (run_cmds + i) - (uintptr_t) pop.handle();
    // }

    // size_t load_direct_offset(size_t i) {
    //     return (uintptr_t) (load_cmds + i) - (uintptr_t) pop.handle();
    // }

    // size_t run_direct_offset(size_t i) {
    //     return (uintptr_t) (run_cmds + i) - (uintptr_t) pop.handle();
    // }


    std::pair<command *, size_t> get_load_command(size_t idx) {
        command *pcmd = proot->get_load_cmd(idx);
        size_t off = (uintptr_t) pcmd - (uintptr_t) pop.handle();
        return {pcmd, off};
    }

    std::pair<command *, size_t> get_run_command(size_t idx) {
        command *pcmd = proot->get_run_cmd(idx);
        size_t off = (uintptr_t) pcmd - (uintptr_t) pop.handle();
        return {pcmd, off};
    }

    static void print_command(ycsb::command const &cmd) {
        std::visit(overload{
                           [&](ycsb::INSERT const &cmd) {
                               fmt::print("INSERT {} {}\n", cmd.key(),
                                          cmd.value());
                           },
                           [&](ycsb::READ const &cmd) {
                               fmt::print("READ {}\n", cmd.key());
                           },
                           [&](ycsb::UPDATE const &cmd) {
                               fmt::print("UPDATE {} {}\n", cmd.key(),
                                          cmd.value());
                           },
                           [&](ycsb::DELETE const &cmd) {
                               fmt::print("DELETE {}\n", cmd.key());
                           },
                           [&](ycsb::CHECK const &cmd) {
                               fmt::print("CHECK {}\n", cmd.key());
                           },
                           [&](ycsb::NONE const &) { fmt::print("NONE\n"); },
                   },
                   cmd);
    }
};

}// namespace pmhb_ns

#endif//PMHB_YCSB_HPP
