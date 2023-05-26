#ifndef PMHB_CONFIG_HPP
#define PMHB_CONFIG_HPP

#include "utils.hpp"

namespace pmhb_ns {
/* Using CPU2 */
// inline constexpr auto LOGIC_PUS =
//         std::array{72,  73,  74,  75,  76,  77,  78,  79,  80,  81,  82,  83,
//                    84,  85,  86,  87,  88,  89,  90,  91,  92,  93,  94,  95,
//                    168, 169, 170, 171, 172, 173, 174, 175, 176, 177, 178, 179,
//                    180, 181, 182, 183, 184, 185, 186, 187, 188, 189, 190, 191};
inline constexpr auto LOGIC_PUS =
        std::array{0,   1,   2,   3,   4,   5,   6,   7,   8,   9,   10,  11,
                   12,  13,  14,  15,  16,  17,  18,  19,  20,  21,  22,  23,
                   96,  97,  98,  99,  100, 101, 102, 103, 104, 105, 106, 107,
                   108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119};

constexpr auto CPU_NUM = LOGIC_PUS.size();

/* The size of every structure */
inline constexpr auto MAP_STRUCTURE_SIZE = 160ul << 30;
inline constexpr auto STRING_LENGTH = 16ul;


// benchmark parameters
struct config {
    bool is_recovery{false};
    size_t thread_num;
    std::filesystem::path working_dir;
    std::filesystem::path output_dir;

    std::filesystem::path ycsb_load_trace, ycsb_run_trace;
    std::filesystem::path pm_ycsb;
    size_t load_num;
    size_t run_num;

    friend std::ostream &operator<<(std::ostream &os, config const &cfg) {
        return os << "{"
                  << fmt::format(
                             "\n\t\"thread_num\": {},\n\t\"is_recovery\": "
                             "\"{}\",\n\t\"working_dir\": "
                             "\"{}\",\n\t\"output_dir\": "
                             "\"{}\",\n\t\"ycsb_load_trace\": "
                             "\"{}\",\n\t\"ycsb_run_trace\": "
                             "\"{}\",\n\t\"pm_ycsb\": \"{}\",\n\t\"load_num\": "
                             "{},\n\t\"run_num\": {}\n",
                             cfg.thread_num, cfg.is_recovery,
                             cfg.working_dir.c_str(), cfg.output_dir.c_str(),
                             cfg.ycsb_load_trace.c_str(),
                             cfg.ycsb_run_trace.c_str(), cfg.pm_ycsb.c_str(),
                             cfg.load_num, cfg.run_num)
                  << "}";
    }
};

}// namespace pmhb_ns

#endif// PMHB_CONFIG_HPP