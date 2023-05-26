#ifndef PMHB_BENCH_HPP
#define PMHB_BENCH_HPP

#include "bench_interface.hpp"
#include "config.hpp"
#include "context.hpp"
#include "performance_profile.hpp"
#include "utils.hpp"
#include "ycsb.hpp"

#include <atomic>
#include <filesystem>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>
//NOLINT
#include <barrier>
#include <fmt/os.h>
#include <fmt/ostream.h>
#include <fmt/ranges.h>


#include <iostream>

// #define CORRECTNESS_CHECK
// #define LOAD_FACTOR

using namespace std::chrono_literals;
namespace pmhb_ns {

// this should be a singleton
// root of the one whole benchmark session
template<typename map_type>
struct bench {
    /* Data */
    inline static bench *g_bench;
    std::unique_ptr<config> cfg;
    std::shared_ptr<bench_interface<map_type>> interface;
    std::unique_ptr<performance_profile> profiler;
    std::unique_ptr<std::barrier<std::__empty_completion>> sync_point;
    std::unique_ptr<ycsb> ycsb_data;
    map_type *table{nullptr};

    std::atomic<size_t> test_pointer = 0;

    /* Constructors */
    bench() = delete;
    bench(bench const &) = delete;
    bench &operator=(bench const &) = delete;
    explicit bench(config const &c,
                   std::shared_ptr<bench_interface<map_type>> _interface)
        : cfg{std::make_unique<config>(c)}, interface(_interface),
          profiler{std::make_unique<performance_profile>()},
          ycsb_data{std::make_unique<ycsb>(*cfg)},
          sync_point(std::make_unique<std::barrier<std::__empty_completion>>(
                  cfg->thread_num)) {
        // cpubind(LOGIC_PUS[cfg->thread_num - 1]);

        /* Avoid to cover the old testing data  */
        // if (std::filesystem::exists(cfg->output_dir)) {
        //     auto bak = cfg->output_dir;
        //     bak += fmt::format("-{}", clock::now().time_since_epoch().count());
        //     std::filesystem::rename(cfg->output_dir, bak);
        // }
        // std::filesystem::create_directory(cfg->output_dir);
        std::filesystem::create_directory(cfg->working_dir);

        fmt::print("creating bench instance with config {}\n", *cfg);
        g_bench = this;
    }

    /* Interfaces */
    static void lights_out() {
        g_bench->open_table();
        g_bench->start();
        g_bench->finish();
    }

private:
    /* Private data */
    std::mutex ctx_init_lock;
    std::atomic<bool> ok_to_start{false};
    std::atomic<size_t> ready_thread_num{0};

    /* Helper class */
    struct perf_guard : time_slice {
        std::string perf_name;

        perf_guard(std::string s) : perf_name(s) {}
        ~perf_guard() {
            elapsed_time = clock::now() - start_time;
            {
                auto g = std::lock_guard(g_bench->profiler->record_lock);
                g_bench->profiler->records.push_back(
                        {perf_name, time_slice{start_time, elapsed_time}});
            }
        }
    };

    // this should only be called in start
    // the reserved thread is for structures that have a background worker, like clevel
    void worker_body(size_t worker_id, size_t reserved_thread_num) {
        const size_t batch_size =
                (cfg->load_num + cfg->run_num > 100000) ? 1000 : 1;
        bool is_main_worker = worker_id == cfg->thread_num - 1;
#ifdef REALTIMETHROUGHPUT
        std::vector<time_point> work_done_point;
        work_done_point.reserve(1'000'000);
#endif
        /* Prepare the environment */
        std::shared_ptr<context> ctx;
        {
            auto g = std::lock_guard{ctx_init_lock};
            auto [pos, succ] = profiler->ctxs.insert(
                    {std::this_thread::get_id(),
                     std::make_shared<context>(4 * cfg->run_num /
                                               cfg->thread_num)});
            ctx = pos->second;
            ctx->tid = worker_id + reserved_thread_num;
        }
        cpubind(LOGIC_PUS[ctx->tid]);
        // fmt::print("worker {} thread {} running on cpu {}\n", worker_id,
        //            pthread_self(), sched_getcpu());
        test_pointer.store(0);


        if (is_main_worker) { fmt::print("Load Phase\n"); }
        sync_point.get()->arrive_and_wait();

        /* LOAD PHASE */
        {
            auto p = perf_guard(
                    fmt::format("load_worker_id {} time_slice", worker_id));
            size_t terminator = cfg->load_num;
            while (true) {
                size_t i = test_pointer.fetch_add(batch_size,
                                                  std::memory_order_relaxed);
                // fmt::print("{} th got the {}th batch\n", ctx->tid, i);
                if (i >= terminator) { break; }
                for (int j = 0; j < batch_size; j++, i++) {
                    if (i >= terminator) { break; }
                    auto [pcmd, offset] = ycsb_data->get_load_command(i);
                    interface->do_ycsb_command(table, ctx.get(), *pcmd, offset,
                                               true);
                }
            }
        }

        sync_point.get()->arrive_and_wait();

        pm_watch *w;
        /* wait or issue the start signal for the second phase: run */
        if (is_main_worker) {
            for (auto &[_, ctx] : profiler->ctxs) {
                ctx->samples.clear();
                ctx->write_count = 0;
            }
            fmt::print("Run Phase\n");
            w = new pm_watch();
            test_pointer.store(0);
        }


        sync_point.get()->arrive_and_wait();

#ifdef CORRECTNESS_CHECK
        /* CHECK PHASE */
        {
            /* Search all KV loaded in LOAD PHASE and check the value */

            // if (is_main_worker) {
            auto p = perf_guard(
                    fmt::format("run_worker_id {} time_slice", worker_id));
            ycsb::CHECK p_cmd;
            size_t terminator = cfg->load_num;
            while (true) {
                size_t i = test_pointer.fetch_add(batch_size,
                                                  std::memory_order_relaxed);
                if (i >= terminator) { break; }
                for (int j = 0; j < batch_size; j++) {
                    auto [pcmd, offset] = ycsb_data->get_load_command(i);
                    new (&p_cmd) ycsb::CHECK(std::get<ycsb::INSERT>(*pcmd));
                    // ycsb::CHECK here
                    interface->do_ycsb_command(table, ctx.get(), p_cmd);
                    if (++i >= terminator) { break; }
                }
#ifdef REALTIMETHROUGHPUT
                work_done_point.push_back(time_point{clock::now()});
#endif
            }
            // }
            // fmt::print("passed the check\n");
        }
#else
        /* RUN PHASE */
        {
            auto p = perf_guard(
                    fmt::format("run_worker_id {} time_slice", worker_id));
            size_t terminator = cfg->run_num;
            double percent = 0.05;
            while (true) {
                size_t i = test_pointer.fetch_add(batch_size,
                                                  std::memory_order_relaxed);
                if (i >= terminator) { break; }
#ifdef LOAD_FACTOR
                // sync point
                if (i >= percent * cfg->run_num) {
                    sync_point.get()->arrive_and_wait();
                    if (is_main_worker) {
                        profiler->load_factors.push_back(interface->load_factor(
                                table, cfg->load_num + percent * cfg->run_num));
                    }
                    percent += 0.05;
                    // skip thessn't arrive this point.
                    if (percent > 0.95) percent = 2.0;
                    sync_point.get()->arrive_and_wait();
                }
#endif
                for (int j = 0; j < batch_size; j++) {
                    auto [pcmd, offset] = ycsb_data->get_run_command(i);

                    interface->do_ycsb_command(table, ctx.get(), *pcmd, offset);
                    if (++i >= terminator) { break; }
                }
#ifdef REALTIMETHROUGHPUT
                work_done_point.push_back(time_point{clock::now()});
#endif
            }
        }
#endif
        // fmt::print("Done\n");
        sync_point.get()->arrive_and_wait();
#ifdef REALTIMETHROUGHPUT
        profiler->realtime_point[worker_id] = work_done_point;
#endif
        if (is_main_worker) { delete w; }
    }

    void open_table() {

        if (!cfg->is_recovery) {
            for (const auto &entry :
                 std::filesystem::directory_iterator(cfg->working_dir))
                std::filesystem::remove_all(entry.path());
            table = interface->do_open(*cfg, ycsb_data->uulo);
            time_log("Opened table");
        } else {
            auto p = perf_guard(
                    fmt::format("recover with {} threads", cfg->thread_num));
            table = interface->do_recover(*cfg);
        }
    }

    void start() {
        std::vector<std::thread> handles{};
        auto worker =
                std::bind(&bench::worker_body, this, std::placeholders::_1, 0);
        for (auto i = 0ul; i < cfg->thread_num - 1; ++i) {
            handles.emplace_back(worker, i);
        }
        /* The main thread that controls the testing */
        worker(cfg->thread_num - 1);
        for (auto &i : handles) { i.join(); }

        interface->do_close(table, *cfg);
        time_log("Test over");
    }

    void finish() {

        /* Store the test result in files */
        // fmt::output_file((cfg->output_dir / "config").c_str())
        //         .print("{}", *cfg);

        /* Data classification */
        auto [op, resize_kv_num, rehash, doubling, write_count] =
                div_ctx(profiler->ctxs);
        fmt::print("op size = {}\n", op.size());

        /* Processing real-time throughput of operations */
        vector<vector<size_t>> rttp =
                gen_realtime_throughput(op, resize_kv_num, doubling);
        if (rttp.size()) {
            // auto stream_rttp =
            //         fmt::output_file((cfg->output_dir / "rttp").c_str());
            // stream_rttp.print("{}\n", rttp);
            fmt::print("\n");
            fmt::print("RTTP = {}\n", rttp.at(0));
            fmt::print("\n");
            fmt::print("RESIZEKV = {}\n", rttp.at(1));
            fmt::print("\n");
            fmt::print("DOUBLING = {}\n", rttp.at(2));
        }

        if (profiler->load_factors.size()) {
            fmt::print("\n");
            fmt::print("LoadFactor = {}\n", profiler->load_factors);
        }

        /* Processing tail latency */
        vector<size_t> tail_latency = gen_tail_latency(op);
        if (tail_latency.size()) {
            // auto stream_tail_latency = fmt::output_file(
            //         (cfg->output_dir / "tail_latency").c_str());
            // stream_tail_latency.print("{}\n", tail_latency);
            fmt::print("\n");
            fmt::print("TailLatency = {}\n", tail_latency);
        }

        /* Processing tail latency */
        // vector<size_t> rehash_latency = gen_tail_latency(rehash);
        if (rehash.size()) {
            size_t sum = 0;
            for (const auto &[time_stamp, elasped] : rehash) { sum += elasped; }
            double average_rehash_latency = sum / rehash.size() / 1e3;
            // fmt::print("\nTotal rehash time is {} s, average is {} us\n",
            //            sum / 1e9, average_rehash_latency);
            fmt::print("\n");
            fmt::print("TotalRehashTime_inSecond = {}\n", sum / 1e9);
            fmt::print("AverageRehashTime_inMicroSecond = {}\n",
                       average_rehash_latency);

            // auto stream_average_rehash_latency = fmt::output_file(
            //         (cfg->output_dir / "average_rehash_latency").c_str());
            // stream_average_rehash_latency.print("{}\n", average_rehash_latency);
        } else if (doubling.size()) {
            size_t sum = 0;
            for (const auto &[time_stamp, elasped] : doubling) {
                sum += elasped;
            }
            fmt::print("\n");
            fmt::print("TotalRehashTime_inSecond = {}\n", sum / 1e9);
        }
        // if (rehash.size()) {
        // vector<size_t> rehash_latency;
        // for (const auto &[time_stamp, elasped] : rehash) {
        // rehash_latency.push_back(elasped);
        // }
        // sort(rehash_latency.begin(), rehash_latency.end());

        // auto stream_rehash_latency = fmt::output_file(
        //         (cfg->output_dir / "rehash_latency").c_str());
        // stream_rehash_latency.print("{}\n", rehash_latency);
        // }

        /* Processing throughput */
        vector<double> throughput =
                gen_throughput(cfg->load_num, cfg->run_num, profiler->records);
        if (throughput.size()) {
            // auto stream_throughput =
            //         fmt::output_file((cfg->output_dir / "throughput").c_str());
            // stream_throughput.print("{}\n", throughput);
            // fmt::print("\nLoad: {}M, Run: {}M\n", throughput[0], throughput[1]);
            fmt::print("\n");
            fmt::print("LoadThroughput_inMops = {}\n", throughput[0]);
            fmt::print("RunThroughput_inMops = {}\n", throughput[1]);
        }

        print_write_count(profiler->ctxs);

#ifdef REALTIMETHROUGHPUT
        vector<size_t> realtime_throughput =
                gen_realtime_throughput_only(profiler->realtime_point);
        if (realtime_throughput.size()) {
            fmt::print("\n");
            fmt::print("RTTP_only = {}\n", realtime_throughput);
        }
#endif

        time_log("Output over");

        for (const auto &entry :
             std::filesystem::directory_iterator(cfg->working_dir))
            std::filesystem::remove_all(entry.path());
    }
};


}// namespace pmhb_ns

#endif//PMHB_BENCH_HPP
