#ifndef PMHB_PERFORMANCE_PROFILE
#define PMHB_PERFORMANCE_PROFILE
#include "context.hpp"
#include "utils.hpp"
#include <fstream>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <tuple>
#include <vector>

#define REALTIMETHROUGHPUT


namespace pmhb_ns {
using std::vector;
/* The data structure to store the performance data */
struct performance_profile {
    std::mutex record_lock;
    vector<std::pair<std::string, time_slice>> records;
    vector<double> load_factors;
    // per-thread context
    std::unordered_map<std::thread::id, std::shared_ptr<context>> ctxs{};
    vector<vector<time_point>> realtime_point = vector<vector<time_point>>(48);
};

/* Data Classification */
std::tuple<vector<std::pair<size_t, size_t>>, vector<std::pair<size_t, size_t>>,
           vector<std::pair<size_t, size_t>>, vector<std::pair<size_t, size_t>>,
           vector<std::pair<size_t, size_t>>>
div_ctx(std::unordered_map<std::thread::id, std::shared_ptr<context>> &ctxs) {
    vector<std::pair<size_t, size_t>> op, resize_kv_num, rehash, doubling,
            write_count;
    op.reserve(4'000'000'000);
    auto visitor = overload{
            [&](INSERT &m) {
                op.push_back({m.start_time.time_since_epoch().count(),
                              m.elapsed_time.count()});
            },
            [&](SEARCH &m) {
                op.push_back({m.start_time.time_since_epoch().count(),
                              m.elapsed_time.count()});
            },
            [&](UPDATE &m) {
                op.push_back({m.start_time.time_since_epoch().count(),
                              m.elapsed_time.count()});
            },
            [&](DELETE &m) {
                op.push_back({m.start_time.time_since_epoch().count(),
                              m.elapsed_time.count()});
            },
            [&](RESIZE_ITEM_NUMBER &m) {
                resize_kv_num.push_back(
                        {m.start_time.time_since_epoch().count(),
                         m.elapsed_time.count()});
            },
            [&](REHASH &m) {
                rehash.push_back({m.start_time.time_since_epoch().count(),
                                  m.elapsed_time.count()});
            },
            [&](DOUBLE &m) {
                doubling.push_back({m.start_time.time_since_epoch().count(),
                                    m.elapsed_time.count()});
            },
            [&](WRITE_COUNT &m) {
                write_count.push_back({m.start_time.time_since_epoch().count(),
                                       m.elapsed_time.count()});
            }};

    for (auto &[_, ctx] : ctxs) {
        // fmt::print("div_samples: lens: {}\n", ctx->samples.size());
        for (auto &sample : ctx->samples) { std::visit(visitor, sample); }
    }

    time_log("div over");
    sort(op.begin(), op.end());
    time_log("op sorted");
    sort(resize_kv_num.begin(), resize_kv_num.end());
    time_log("resize num sorted");
    sort(rehash.begin(), rehash.end());
    time_log("rehash sorted");
    sort(doubling.begin(), doubling.end());
    sort(write_count.begin(), write_count.end());
    return {op, resize_kv_num, rehash, doubling, write_count};
}

/* Processing the performance data */
vector<vector<size_t>>
gen_realtime_throughput(vector<std::pair<size_t, size_t>> &op,
                        vector<std::pair<size_t, size_t>> &resize_kv_num,
                        vector<std::pair<size_t, size_t>> &doubling) {

    // for ()

    if (op.empty()) { return {}; }

    /* Create the time line for query & involved_kv */

    size_t minimal_timestamp = op.front().first;
    size_t maximal_timestamp = op.back().first;

    if (resize_kv_num.size()) {
        minimal_timestamp =
                std::min(minimal_timestamp, resize_kv_num.front().first);
        maximal_timestamp =
                std::max(maximal_timestamp, resize_kv_num.back().first);
    }

    size_t time_interval = (size_t) 1e8;
    size_t N = (maximal_timestamp - minimal_timestamp + time_interval - 1) /
               time_interval;
    vector<size_t> time_line_operation(N, 0);
    vector<size_t> time_line_kv(N, 0);
    vector<size_t> time_line_doubling(N, 0);

    /* Increase the value of timelines */
    for (const auto &[time_stamp, elapsed] : op) {
        size_t idx = (time_stamp - minimal_timestamp) / time_interval;
        ++time_line_operation.at(idx);
    }

    for (const auto &[time_stamp, kv_num] : resize_kv_num) {
        size_t idx = (time_stamp - minimal_timestamp) / time_interval;
        time_line_kv.at(idx) += kv_num;
    }


    for (const auto &[time_stamp, doubling_time] : doubling) {
        size_t idx = (time_stamp - minimal_timestamp) / time_interval;
        if (idx >= 0 && idx < time_line_doubling.size())
            time_line_doubling.at(idx) += doubling_time;
    }

    /* Return the result */
    for (auto &n : time_line_operation) { n *= lround(1e9 / time_interval); }
    for (auto &n : time_line_kv) { n *= lround(1e9 / time_interval); }

    return {time_line_operation, time_line_kv, time_line_doubling};
}

vector<size_t> gen_tail_latency(vector<std::pair<size_t, size_t>> &op) {
    vector<size_t> latency;
    for (auto const &[time_stamp, elasped] : op) { latency.push_back(elasped); }

    if (latency.empty()) { return {}; }

    /* Create the time line for query & involved_kv */
    std::sort(latency.begin(), latency.end());


    vector<size_t> point_latency;
    /* 50, 75, 90, 99, 99.9, 99.99, 99.999, 100 */

    for (auto point : {0.5, 0.75, 0.9, 0.99, 0.999, 0.9999, 0.99999, 1.0}) {
        size_t idx = (size_t) (latency.size() * point - 1);
        point_latency.push_back(latency.at(idx));
    }

    return point_latency;
}

vector<double>
gen_throughput(size_t load_op, size_t run_op,
               vector<std::pair<std::string, time_slice>> &records) {
    if (records.empty()) return {};
    vector<size_t> load_time, run_time;
    for (auto &r : records) {
        if (r.first.find("load") != std::string::npos) {
            load_time.push_back(r.second.elapsed_time.count());
        }
        if (r.first.find("run") != std::string::npos) {
            run_time.push_back(r.second.elapsed_time.count());
        }
    }
    double average_load_time =
            std::accumulate(load_time.begin(), load_time.end(), 0.0) /
            load_time.size() / 1e9;
    double average_run_time =
            std::accumulate(run_time.begin(), run_time.end(), 0.0) /
            run_time.size() / 1e9;
    return {load_op / average_load_time / 1e6, run_op / average_run_time / 1e6};
}

void print_write_count(
        std::unordered_map<std::thread::id, std::shared_ptr<context>> &ctxs) {
    size_t sum_write = 0;
    for (auto &[_, ctx] : ctxs) { sum_write += ctx->write_count; }
    fmt::print("total write_count is {}\n", sum_write);
    return;
}

vector<size_t>
gen_realtime_throughput_only(vector<vector<time_point>> &realtime_point) {
    vector<size_t> all_time_point;
    all_time_point.reserve(1'000'000'000);

    for (const auto &v : realtime_point) {
        for (const auto &t : v) {
            all_time_point.push_back(t.time_since_epoch().count());
        }
    }

    sort(all_time_point.begin(), all_time_point.end());

    size_t time_interval = (size_t) 1e8;
    const size_t minimal_timestamp = all_time_point.front();
    const size_t maximal_timestamp = all_time_point.back();

    size_t N = (maximal_timestamp - minimal_timestamp + time_interval - 1) /
               time_interval;
    vector<size_t> time_line_operation(N, 0);

    /* Increase the value of timelines */
    for (const auto &t : all_time_point) {
        size_t idx = (t - minimal_timestamp) / time_interval;
        ++time_line_operation.at(idx);
    }

    /* Return the result */
    // 每个时间点代表处理了1000个数据,与bench对应
    for (auto &n : time_line_operation) {
        n *= lround(1e9 / time_interval) * 1000;
    }

    return time_line_operation;
}


struct pm_data {
public:
    std::vector<double> data = {0.0, 0.0, 0.0, 0.0};
    void get_data() {
        int r = system("sudo ipmctl show -performance > pm_data.txt");
        if (r) exit(r);

        std::ifstream infile("pm_data.txt");

        // read the data Dimm by Dimm
        std::string dimm_name;
        while (std::getline(infile, dimm_name)) {
            auto dimm_data = get_dimm_data(infile);

            if (dimm_name.at(0) != '-') { exit(1); }
            std::string socket_number =
                    dimm_name.substr(dimm_name.find('=') + 1, 3);
            // Only calculate socket 0
            if (socket_number == "0x0") {
                for (int i = 0; i < 4; i++) { data.at(i) += dimm_data.at(i); }
            }
        }
    }

private:
    std::vector<double> get_dimm_data(std::ifstream &infile) {
        std::vector<double> result;
        for (int i = 0; i < 8; i++) {
            result.push_back(get_oneline_performance(infile));
        }
        return result;
    }

    double get_oneline_performance(std::ifstream &infile) {
        std::string data;
        std::getline(infile, data);
        data = data.substr(data.find('=') + 1);
        return std::stod(data) * 64;
    }
};

struct pm_watch {
private:
    pm_data start, end;

public:
    pm_watch() { start.get_data(); }
    ~pm_watch() {
        end.get_data();
        std::vector<std::string> name = {"Media Read", "Media Write",
                                         "IMC Read", "IMC Write"};
        std::vector<double> value;
        for (int i = 0; i < 4; i++) {
            value.push_back((end.data.at(i) - start.data.at(i)) / 1024 / 1024);
        }
        value.at(0) -= value.at(1);
        // this code because https://github.com/intel/intel-pmwatch/blob/master/docs/PMWatch_User_Guide.pdf

        fmt::print("---------------------------\n");
        for (int i = 0; i < 4; i++)
            fmt::print("{} : {:.2f} MB\n", name.at(i), value.at(i));
        fmt::print("---------------------------\n");
        fmt::print("MediaWrite_inMB = {}\n", value.at(1));
    }
};


}// namespace pmhb_ns


#endif