#ifndef PCLHT_MAKE_PERSISTENT_OBJECT_HPP
#define PCLHT_MAKE_PERSISTENT_OBJECT_HPP

#include <libpmemobj++/detail/common.hpp>
#include <libpmemobj++/detail/persistent_pool_ptr.hpp>
#include <libpmemobj++/detail/specialization.hpp>
#include <libpmemobj++/detail/template_helpers.hpp>
#include <libpmemobj++/experimental/v.hpp>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/make_persistent_atomic.hpp>
#include <libpmemobj++/mutex.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/transaction.hpp>

//inline static auto clevel_allocation_counter = 0ul;
#define LIBPMEMOBJ_CPP_CONCURRENT_HASH_MAP_USE_ATOMIC_ALLOCATOR 1
namespace pclht_ns::internal {
using namespace pmem::obj;
/**
 * Wrapper around PMDK allocator
 * @throw std::bad_alloc on allocation failure.
 */
template<typename T, typename U, typename... Args>
void make_persistent_object(pool_base &pop, persistent_ptr<U> &ptr,
                            Args &&... args) {
#if LIBPMEMOBJ_CPP_CONCURRENT_HASH_MAP_USE_ATOMIC_ALLOCATOR
    make_persistent_atomic<T>(pop, ptr, std::forward<Args>(args)...);
#else
    transaction::manual tx(pop);
    persistent_ptr<T> p_kv;
    p_kv = make_persistent<T>(std::forward<Args>(args)...);
    ptr = p_kv;
    transaction::commit();
//    ++clevel_allocation_counter;
#endif
}
}// namespace pclht_ns::internal

#endif//PCLHT_MAKE_PERSISTENT_OBJECT_HPP
