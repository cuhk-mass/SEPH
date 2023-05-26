#include "store.hpp"
#include "util.hpp"
#include "ycsb.hpp"

int main(int argc, char *argv[]) {
  if (argc != 4) {
    fmt::print("cmd [src_load] [src_run] [dst_pm]\n");
    return 0;
  }
  filesystem::path src_load{argv[1]};
  filesystem::path src_run{argv[2]};
  filesystem::path dst_pm{argv[3]};

  vector<command> cmds;
  cmds.reserve(4'000'000'000);
  size_t load_num = 0, run_num = 0;

  time_log("Start to parse\n");

  load_num = parse_ycsb(cmds, src_load);
  time_log(fmt::format("Parsed load_cmds: {}\n", load_num));
  run_num = parse_ycsb(cmds, src_run);
  time_log(fmt::format("Parsed run_cmds: {}\n", run_num));

  time_log("finish parsing\n");

  store(dst_pm, cmds, load_num, run_num);
  time_log("Stored into PM\n");
  return 0;
}