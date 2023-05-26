#ifndef STEPH_CONFIG_HPP
#define STEPH_CONFIG_HPP

#include <cstddef>

inline constexpr size_t DEFAULT_POOL_SIZE = 16ul << 30;


#ifndef TRADITIONAL_LOCK
inline constexpr auto KV_NUM_PER_BUCKET = 32ul;
#else
inline constexpr auto KV_NUM_PER_BUCKET = 31ul;
#endif
/* To tune the performance, find the optimal segment size */
inline constexpr auto BUCKET_INDEX_BIT_NUM = 6ul;
inline constexpr auto BUCKET_NUM_PER_SEGMENT = 1ul << BUCKET_INDEX_BIT_NUM;
inline constexpr auto SEGMENT_POOL_PATH = "/mnt/pmem0/Testee/sh.seg";
// inline constexpr auto SEGMENT_POOL_SIZE = DEFAULT_POOL_SIZE << 30ul;
inline constexpr auto SH_POOL_PATH = "/mnt/pmem0/Testee/sh";
// inline constexpr auto SH_POOL_SIZE = DEFAULT_POOL_SIZE << 30ul;
inline constexpr auto SH_POOL_LAYOUT = "sh";
inline constexpr auto KV_POOL_PATH = "/mnt/pmem0/Testee/sh.kv";
// inline constexpr auto KV_POOL_SIZE = DEFAULT_POOL_SIZE << 30ul;
inline constexpr auto KV_POOL_LAYOUT = "sh";

inline constexpr auto FINGERPRINT_BIT_ALIGNMENT = 8ul;

#endif//STEPH_CONFIG_HPP
