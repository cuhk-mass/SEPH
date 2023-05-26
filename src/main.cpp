#include "adapter/cceh_cow_interface.hpp"
#include "adapter/cceh_interface.hpp"
#include "adapter/clevel_interface.hpp"
#include "adapter/dash_interface.hpp"
// #include "adapter/dummy_interface.hpp"
#include "adapter/level_interface.hpp"
#include "adapter/pclht_interface.hpp"
#include "adapter/steph_interface.hpp"
#include "bench.hpp"
#include <cxxopts.hpp>
#include <string>


int main(int argc, char *argv[]) {
    auto opts =
            cxxopts::Options{"pmhb", "Persistent Memory Hashtable Benchmark"};
    opts.add_options()("h,help", "Print usage")("v,verbose", "Verbose output");
    opts.add_options()("e,hash_scheme",
                       "Which hashing scheme to benchmark. Possible values: "
                       "steph, dash, level, cceh, cceh_cow, level, clht",
                       cxxopts::value<std::string>()->default_value("steph"));
    opts.add_options()("t,thread_num", "Thread number",
                       cxxopts::value<size_t>()->default_value("1"));
    opts.add_options()("w,working_dir",
                       "Working directory. Persistent memory pools and/or "
                       "memory mapped files will be created here.",
                       cxxopts::value<std::filesystem::path>()->default_value(
                               "/mnt/pmem0/Testee"));
    opts.add_options()(
            "o,output_dir",
            "Output directory. Captured data samples will be placed here.",
            cxxopts::value<std::filesystem::path>()->default_value(
                    "./data/result"));
    opts.add_options()("l,ycsb_load",
                       "Load phase file. It should be a ycsb workload "
                       "generated for BasicDB or in simplified k/v format",
                       cxxopts::value<std::filesystem::path>()->default_value(
                               "/dev/null"));
    opts.add_options()("r,ycsb_run",
                       "Run phase file. It should be a ycsb workload generated "
                       "for BasicDB or in simplified k/v format",
                       cxxopts::value<std::filesystem::path>()->default_value(
                               "/dev/null"));
    opts.add_options()("m,pm_ycsb",
                       "Run phase file. It should be a ycsb workload generated "
                       "for BasicDB or in simplified k/v format",
                       cxxopts::value<std::filesystem::path>()->default_value(
                               "/dev/null"));
    opts.add_options()("load_num", "Data size to load",
                       cxxopts::value<size_t>()->default_value("100000000"));
    opts.add_options()("run_num", "Data size to run",
                       cxxopts::value<size_t>()->default_value("100000000"));
    opts.add_options()("R,recovery",
                       "create a new test or recover from existing pools",
                       cxxopts::value<bool>()->default_value("false"));
    auto args = opts.parse(argc, argv);
    if (args.count("help")) {
        std::cout << opts.help() << std::endl;
        return 0;
    }


    auto cfg = pmhb_ns::config{
            args["recovery"].as<bool>(),
            args["thread_num"].as<size_t>(),
            args["working_dir"].as<std::filesystem::path>(),
            args["output_dir"].as<std::filesystem::path>() /
                    fmt::format("{}-{}", args["hash_scheme"].as<std::string>(),
                                args["thread_num"].as<size_t>()),
            args["ycsb_load"].as<std::filesystem::path>(),
            args["ycsb_run"].as<std::filesystem::path>(),
            args["pm_ycsb"].as<std::filesystem::path>(),
            args["load_num"].as<size_t>(),
            args["run_num"].as<size_t>()};

    auto scheme = args["hash_scheme"].as<std::string>();
    if (scheme == "steph") {
        auto b = pmhb_ns::bench<pmhb_ns::adapter::steph_map_type>{
                cfg, std::make_shared<pmhb_ns::adapter::steph>()};
        b.lights_out();
    } else if (scheme == "level") {
        auto b = pmhb_ns::bench<pmhb_ns::adapter::level::map_type>{
                cfg, std::make_shared<pmhb_ns::adapter::level>()};
        b.lights_out();
    } else if (scheme == "cceh") {
        auto b = pmhb_ns::bench<pmhb_ns::adapter::cceh::map_type>{
                cfg, std::make_shared<pmhb_ns::adapter::cceh>()};
        b.lights_out();
    } else if (scheme == "cceh_cow") {
        auto b = pmhb_ns::bench<pmhb_ns::adapter::cceh_cow::map_type>{
                cfg, std::make_shared<pmhb_ns::adapter::cceh_cow>()};
        b.lights_out();
    } else if (scheme == "dash") {
        auto b = pmhb_ns::bench<pmhb_ns::adapter::dash::map_type>{
                cfg, std::make_shared<pmhb_ns::adapter::dash>()};
        b.lights_out();
    } else if (scheme == "clevel") {
        auto b = pmhb_ns::bench<pmhb_ns::adapter::clevel::map_type>{
                cfg, std::make_shared<pmhb_ns::adapter::clevel>()};
        b.lights_out();
    } else if (scheme == "pclht") {
        auto b = pmhb_ns::bench<pmhb_ns::adapter::clht::map_type>{
                cfg, std::make_shared<pmhb_ns::adapter::clht>()};
        b.lights_out();
    } else {
        std::cout << opts.help() << std::endl;
    }

    return 0;
}
