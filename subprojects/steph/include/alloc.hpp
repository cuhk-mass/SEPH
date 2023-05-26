#ifndef STEPH_ALLOC_HPP
#define STEPH_ALLOC_HPP

#include <cmath>
#include <filesystem>
#include <fmt/core.h>
#include <libpmem.h>
#include <mutex>

#include "util.hpp"
namespace steph_ns {

// must be located at the beginning of a pmem pool
template<typename T>
struct stack_allocator {
    char magic[32];
    size_t offset;
    std::mutex mutex;
    size_t length;

    stack_allocator() = delete;
    stack_allocator(stack_allocator const &) = delete;
    stack_allocator(stack_allocator &&) = delete;


    static void close(stack_allocator *that) {
        auto ret = pmem_unmap(that, that->length);
        if (ret != 0) {
            fmt::print("pmem_unmap returned {} {}", ret, errno);
            throw std::runtime_error(pmem_errormsg());
        }
    }

    static stack_allocator *open(char const *path, size_t len) {
        stack_allocator *ret = nullptr;

        size_t mapped_len;
        int is_pmem;
        if (std::filesystem::exists(path)) {
            fmt::print("stack_allocator {} exists and has been removed ({})\n",
                       path, std::filesystem::remove(path));
            // if ((ret = reinterpret_cast<stack_allocator *>(
            //              pmem_map_file(path, 0, PMEM_FILE_EXCL, 0666,
            //                            &mapped_len, &is_pmem))) == nullptr) {
            //     throw std::runtime_error(pmem_errormsg());
            // }
            // {
            //     time_guard tg("Memset the segment pool");
            //     fmt::print("pool size is {:2e}\n", (double) len);
            //     /* To eliminate page fault for the segment pool */
            //     pre_fault(ret, mapped_len);
            // }
        }

        fmt::print("stack_allocator creates {}\n", path);
        if ((ret = reinterpret_cast<stack_allocator *>(
                     pmem_map_file(path, len, PMEM_FILE_CREATE | PMEM_FILE_EXCL,
                                   0666, &mapped_len, &is_pmem))) == nullptr) {
            throw std::runtime_error(pmem_errormsg());
        }
        {
            time_guard tg("Memset the segment pool");
            fmt::print("pool size is {:2e}\n", (double) len);
            /* To eliminate page fault for the segment pool */
            pre_fault(ret, len);
        }

        // yet to be initialized
        if (strcmp(ret->magic, "STACK_ALLOCATOR")) {
            fmt::print("stack_allocator {} element {}", sizeof(stack_allocator),
                       sizeof(T));
            ret->offset = static_cast<size_t>(std::ceil(
                    static_cast<double>(sizeof(stack_allocator)) / sizeof(T)));
            pmem_persist(&ret->offset, sizeof(size_t));
            strcpy(ret->magic, "STACK_ALLOCATOR");
            pmem_persist(ret->magic, sizeof(ret->magic));

            fmt::print("allocator created");
        } else {
            fmt::print("allocator opened");
        }

        ret->mutex.unlock();
        pmem_persist(&ret->mutex, sizeof(ret->mutex));
        fmt::print(": pmem address space [{}, {}) element size {} initial "
                   "offset {} \n",
                   (void *) ret, (void *) ((char *) ret + mapped_len),
                   sizeof(T), ret->offset);
        ret->length = mapped_len;
        return ret;
    }

    // usage:
    // auto [addr, offset] = allocator.alloc();
    std::tuple<T *, size_t> alloc() {
        // auto g = std::lock_guard(mutex);
        // offset += 1;
        // size_t new_off = offset + 1;
        // __sync_bool_compare_and_swap(&offset, new_off - 1, new_off);
        size_t tmp = __atomic_add_fetch(&offset, 1, __ATOMIC_RELAXED);
        pmem_persist(&offset, sizeof(size_t));
        return {reinterpret_cast<T *>(this) + tmp - 1, tmp - 1};
    }

    void clear() {
        // auto g = std::lock_guard(mutex);
        offset = static_cast<size_t>(std::ceil(
                static_cast<double>(sizeof(stack_allocator)) / sizeof(T)));
        pmem_persist(&offset, sizeof(size_t));
    }
};
}// namespace steph_ns


#endif//STEPH_ALLOC_HPP
