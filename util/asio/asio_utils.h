// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <array>
#include <type_traits>

#include <boost/asio/buffer.hpp>
#include "base/pod_array.h"

namespace boost {
namespace asio {

inline mutable_buffer buffer(base::PODArray<uint8_t>& arr) noexcept {
  return mutable_buffer(arr.data(), arr.size());
}

inline const_buffer buffer(const base::PODArray<uint8_t>& arr) noexcept {
  return const_buffer(arr.data(), arr.size());
}

}  // namespace asio
}  // namespace boost

namespace util {

namespace detail {

// Not implemented on purpose since used only in decltype context.
template <typename... T>
constexpr auto _MakeCommonBuf(T&&... values) ->
        typename std::common_type<decltype(::boost::asio::buffer(values))...>::type;

}  // namespace detail

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wmissing-braces"

template <typename... T>
constexpr auto make_buffer_seq(T&&... values) ->
        std::array<decltype(detail::_MakeCommonBuf(values...)), sizeof...(T)> {
    return {::boost::asio::buffer(values)...};
}
#pragma clang diagnostic pop

}  // namespace util
