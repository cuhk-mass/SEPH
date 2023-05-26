// #include <fmt/core.h>
// using fmt::print;

#include <string>
#include <string_view>

#include "../../include/sample_guard.hpp"

namespace dummy_ns {

template<typename KV>
struct dummy {
public:
    dummy(size_t l_) : level(l_) {}
    static dummy *Open() { return new dummy<KV>(8); }
    static dummy *Recover() { return new dummy<KV>(8); }
    void *Insert(std::string_view k_in, std::string_view v_in, size_t pkv = 0) {
        auto g = pmhb_ns::sample_guard<dummy<KV>, pmhb_ns::INSERT>();
#ifdef COUNTING_WRITE
        auto g1 = pmhb_ns::sample_guard<dummy<KV>, pmhb_ns::WRITE_COUNT>(100);
#endif
        if (pmhb_ns::clock::now().time_since_epoch().count() % 10 == 0) {
            auto g2 = pmhb_ns::sample_guard<dummy<KV>, pmhb_ns::REHASH>();
        }
        if (pmhb_ns::clock::now().time_since_epoch().count() % 100 == 0) {
            auto g3 = pmhb_ns::sample_guard<dummy<KV>, pmhb_ns::DOUBLE>();
        }
        return nullptr;
    }
    void *Search(std::string_view k_in) {
        auto g = pmhb_ns::sample_guard<dummy<KV>, pmhb_ns::SEARCH>();
        return nullptr;
    }
    void *Update(std::string_view k_in, std::string_view v_in) {
        auto g = pmhb_ns::sample_guard<dummy<KV>, pmhb_ns::UPDATE>();
        return nullptr;
    }
    void *Delete(std::string_view k_in) {
        auto g = pmhb_ns::sample_guard<dummy<KV>, pmhb_ns::DELETE>();
        return nullptr;
    }
    double Load_factor() { return 1.0; }

private:
    size_t level;
};


}// namespace dummy_ns
