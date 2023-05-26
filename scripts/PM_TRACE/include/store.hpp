#pragma once
#include "util.hpp"

#include "cmd_list.hpp"

void store(filesystem::path target_file, vector<command> &cmds,
           const size_t load_num, const size_t run_num) {
  /* The size should be flexsible */
  // calculate size for the pool
  const size_t total_cmd_number = load_num + run_num;
  const size_t segment_num_on_demand =
      (total_cmd_number + CMD_SEGMENT_CAPACITY - 1) / CMD_SEGMENT_CAPACITY;

  size_t file_size =
      8 * 1024 * 1024 + max((size_t)(2 * total_cmd_number * sizeof(command)),
                            (size_t)(CMD_SEGMENT_CAPACITY * sizeof(command) *
                                     segment_num_on_demand * 1.1));

  if (CMD_SEGMENT_NUM * CMD_SEGMENT_CAPACITY < total_cmd_number) {
    fmt::print(
        "The parameter of SEGMENT in the PM pool is smaller than expected\n");
    exit(1);
  }

  if (CMD_SEGMENT_CAPACITY * sizeof(command) >= 16ul * 1024 * 1024 * 1024) {
    fmt::print("The size of SEGMENT is larger than 16GB\n");
    exit(1);
  }

  if (filesystem::exists(target_file)) {
    fmt::print("Removed existing target_file: {}\n",
               filesystem::remove(target_file));
  }

  // 1. create a file

  pmem::obj::pool<root_type> pop;
  try {
    pop = pop.create(target_file.c_str(), "KV", file_size, 0600);
  } catch (const exception &e) {
    cerr << e.what() << '\n';
    exit(1);
  }
  fmt::print("File is mapped from {} to {}\n", (void *)pop.handle(),
             (void *)((char *)pop.handle() + file_size));

  // 2. write the root for the file
  root_type &root = *pop.root();

  root.load_cmd_num = load_num;
  root.run_cmd_num = run_num;

  if (segment_num_on_demand > CMD_SEGMENT_NUM) {
    fmt::print("the demanding segment is larger than the upper bound\n");
    exit(1);
  }

  fmt::print("command_number is {}, so {} segments are needed\n",
             total_cmd_number, segment_num_on_demand);

  for (size_t i = 0; i < segment_num_on_demand; ++i) {
    int alloc_ret = pmemobj_alloc(pop.handle(), &root.command_segments[i],
                                  CMD_SEGMENT_CAPACITY * sizeof(command), 233,
                                  nullptr, nullptr);
    if (alloc_ret != 0) {
      fmt::print("Segment[{}] allocation failed and returned {}\n", i,
                 alloc_ret);
      exit(1);
    } else {
      fmt::print("Segment[{}] is mapped from {} to {}\n", i,
                 (void *)pmemobj_direct(root.command_segments[i]),
                 (void *)((char *)pmemobj_direct(root.command_segments[i]) +
                          CMD_SEGMENT_CAPACITY * sizeof(command)));
    }
  }
  pmem_persist(&root, sizeof(root_type));

  // 3. write the command list to the file, segment by segment.

  for (size_t seg_idx = 0; seg_idx < segment_num_on_demand; ++seg_idx) {
    command *segment =
        (command *)pmemobj_direct(root.command_segments[seg_idx]);
    size_t size_to_copy = min(CMD_SEGMENT_CAPACITY,
                              cmds.size() - seg_idx * CMD_SEGMENT_CAPACITY) *
                          sizeof(command);
    pmem_memcpy_persist(segment, cmds.data() + seg_idx * CMD_SEGMENT_CAPACITY,
                        size_to_copy);
    time_log(fmt::format("segment {}/{} is ready", seg_idx + 1,
                         segment_num_on_demand));
  }
  time_log(fmt::format("Target file {} is ready\n", target_file.c_str()));

  // 4. close the file
  pop.close();
}
