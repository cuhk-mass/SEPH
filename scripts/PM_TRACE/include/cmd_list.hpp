#pragma once
#include "util.hpp"

#include "varlen_kv.hpp"

constexpr size_t STRING_LENGTH = 16;

struct INSERT : varlen_kv {
  INSERT(std::string_view k, std::string_view v)
      : varlen_kv(STRING_LENGTH, k, STRING_LENGTH, v) {}

private:
  char padding[2 * STRING_LENGTH];
};
struct READ : varlen_kv {
  READ(std::string_view k) : varlen_kv(STRING_LENGTH, k) {}

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
// using root_type = std::array<command, MAX_KV_IN_PM>;

constexpr size_t CMD_SEGMENT_CAPACITY = 256ul << 20;

constexpr size_t CMD_SEGMENT_NUM = 64;

struct root_type {
  size_t load_cmd_num = 0;
  size_t run_cmd_num = 0;
  PMEMoid command_segments[CMD_SEGMENT_NUM];

  /* Function */

  void *get_load_cmd_addr(size_t idx) {
    size_t seg_idx = idx / CMD_SEGMENT_CAPACITY;
    size_t slot_idx = idx % CMD_SEGMENT_CAPACITY;
    return (command *)(pmemobj_direct(command_segments[seg_idx])) + slot_idx;
  }

  void *get_run_cmd_addr(size_t idx) {
    idx += load_cmd_num;
    size_t seg_idx = idx / CMD_SEGMENT_CAPACITY;
    size_t slot_idx = idx % CMD_SEGMENT_CAPACITY;
    return (command *)(pmemobj_direct(command_segments[seg_idx])) + slot_idx;
  }
};

void init_cmds(vector<command> &cmds, size_t num) {
  fmt::print("Now in init_cmds\n");
  for (size_t i = 0; i < num; ++i) {
    cmds.push_back(READ(string("")));
    if (i % 10'000'000 == 0) {
      fmt::print("init {} Million commands\n", i / 1'000'000);
    }
  }
  fmt::print("init_cmds end with {} commands\n", cmds.size());
}