#pragma once

#include <bits/stdc++.h>
#include <fmt/chrono.h>
#include <fmt/core.h>
#include <fmt/os.h>
#include <fmt/ranges.h>
#include <libpmem.h>
#include <libpmemobj++/make_persistent_array_atomic.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj.h>
using namespace std;

template <class... Ts> struct overload : Ts... { using Ts::operator()...; };
template <class... Ts> overload(Ts...) -> overload<Ts...>;

void time_log(const string_view info) {
  fmt::print("[{:%H:%M:%S}] {}\n", fmt::localtime(time(nullptr)), info.data());
}
