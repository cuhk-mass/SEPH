#ifndef PMHB_VARLEN_KV_HPP
#define PMHB_VARLEN_KV_HPP

#include <array>
#include <cctype>
#include <cstring>
#include <string>
#include <string_view>
#include <tuple>

#pragma GCC diagnostic ignored "-Warray-bounds"
#pragma GCC diagnostic ignored "-Wstringop-overflow="

struct varlen_kv {
public:
    /* Types */
    using K_TYPE = std::array<char, 16>;
    using V_TYPE = std::array<char, 16>;
    using construct_type =
            std::tuple<unsigned, std::string_view, unsigned, std::string_view>;
    /* Constructors */
    varlen_kv() : key_size(0), value_size(0) {}
    varlen_kv(varlen_kv const &_kv)
        : key_size(_kv.key_size), value_size(_kv.value_size) {
        if (key_size) {
            strncpy(key(), _kv.key(), key_size);
            key()[key_size - 1] = 0;
        }

        if (value_size) {
            strncpy(value(), _kv.value(), value_size);
            value()[value_size - 1] = 0;
        }
    }

    varlen_kv(unsigned _key_size, std::string_view _key,
              unsigned _value_size = 0, std::string_view _value = "")
        : key_size(_key_size), value_size(_value_size) {
        strncpy(key(), _key.data(), key_size - 1);
        key()[key_size - 1] = 0;
        if (_value_size) {
            strncpy(value(), _value.data(), value_size - 1);
            value()[value_size - 1] = 0;
        }
    }

    varlen_kv(void *_arg) {
        auto arg = reinterpret_cast<construct_type *>(_arg);
        new (this) varlen_kv(std::get<0>(*arg), std::get<1>(*arg),
                             std::get<2>(*arg), std::get<3>(*arg));
    }

    void construct(unsigned _key_size, std::string_view _key,
                   unsigned _value_size = 0, std::string_view _value = "") {
        strncpy(key(), _key.data(), key_size - 1);
        key()[key_size - 1] = 0;
        if (_value_size) [[likely]] {
            strncpy(value(), _value.data(), value_size - 1);
            value()[value_size - 1] = 0;
        }
    }

    /* Interfaces */
    char *key() { return c_str(); }
    char *value() { return c_str() + key_size; }
    const char *key() const { return cc_str(); }
    const char *value() const { return cc_str() + key_size; }
    unsigned size() const { return sizeof(varlen_kv) + key_size + value_size; }

private:
    /* Data members */
    unsigned key_size;
    unsigned value_size;

    char *c_str() { return ((char *) this) + sizeof(varlen_kv); }
    const char *cc_str() const { return ((char *) this) + sizeof(varlen_kv); }
};

#endif