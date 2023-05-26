#pragma once
#include "util.hpp"

#include "cmd_list.hpp"

/**
 * @brief A YCSB line -> a "command" object
 *      (Invalid YCSB line -> "None")
 *
 * @param sv
 * @return command
 */
command make_command(string_view sv) {
  auto op = sv.at(0);
  if (!isupper(op))
    return NONE{};

  sv.remove_prefix(sv.find_first_of(' '));
  sv.remove_prefix(size("usertable user"));
  auto k = sv.substr(0, min(STRING_LENGTH - 1, sv.find_first_of(' ')));

  if (op == 'I') {
    // INSERT usertable user4876795174170569834 [ field1=...
    auto v = sv.substr(sv.find_first_of('=') + 1, STRING_LENGTH - 1);
    // fmt::print("insert: {} {}\n", string(k), string(v));
    return INSERT(string(k), string(v));
  } else if (op == 'R') {
    // READ usertable user763935630407983107 [ <all fields>]
    return READ(string(k));
  } else if (op == 'U') {
    // UPDATE usertable user7456195669291483653 [ field4=...
    // UPDATE usertable user5544792671895998891 [ field0=#U#=L/0<n>6d'K9 ]
    auto v = sv.substr(sv.find_first_of('=') + 1, STRING_LENGTH - 1);
    return UPDATE(string(k), string(v));
  } else if (op == 'D') {
    // TODO: NOT IMPLEMENTED, temporarily use READ format instead
    // DELETE usertable user763935630407983107 [ <all fields>]
    return DELETE(string(k));
  }
  return NONE{};
}

/**
 * @brief A YCSB file -> a list of "command"
 *
 * @param cmds : command list to store "command" object
 * @param path : YCSB file to parse
 */
size_t parse_ycsb(vector<command> &cmds, filesystem::path path) {
  auto stream = ifstream{path};
  auto line = string{};
  string accepted_cmd = "IRUD";
  size_t cnt = 0;
  size_t cmd_cnt = 0;
  while (getline(stream, line)) {
    cnt++;
    if (cnt % 10'000'000 == 0) {
      fmt::print("{}: {} Million\n", path.c_str(), cnt / 1'000'000);
    }
    if (accepted_cmd.find(line.at(0)) != string::npos) {
      auto cmd = make_command(line);
      if (cmd.index() != command(NONE{}).index()) {
        cmds.push_back(cmd);
        ++cmd_cnt;
      }
    }
  }
  return cmd_cnt;
}