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

inline mutable_buffer* buffer_sequence_begin(mutable_buffer& b) {
  return &b;
}

/// Get an iterator to the first element in a buffer sequence.
inline const_buffer* buffer_sequence_begin(const_buffer& b) {
  return &b;
}

inline mutable_buffer* buffer_sequence_end(mutable_buffer& b) {
  return &b + 1;
}

/// Get an iterator to the first element in a buffer sequence.
inline const_buffer* buffer_sequence_end(const_buffer& b) {
  return &b + 1;
}

}  // namespace asio
}  // namespace boost

namespace util {

template<typename It> class ContainerSlice {
 public:
  typedef It iterator;

  ContainerSlice() : b_(), e_() {}
  ContainerSlice(It b, It e) : b_(b), e_(e) {}

  It begin() const noexcept { return b_;}
  It end() const noexcept { return e_;}
  size_t size() const { return e_ - b_; }
 private:
  It b_, e_;
};

namespace detail {

// Not implemented on purpose since used only in decltype context.
template <typename... T>
constexpr auto _MakeCommonBuf(T&&... values) ->
        typename std::common_type<decltype(::boost::asio::buffer(values))...>::type;

template <typename BufferSequence> struct BufferSequenceTraits {
  using raw_it_t = decltype(::boost::asio::buffer_sequence_begin(*static_cast<BufferSequence*>(0)));
  using iterator = std::conditional_t<std::is_pointer<raw_it_t>::value,
                                      std::add_pointer_t<std::remove_const_t<std::remove_pointer_t<raw_it_t>>>,
                                      raw_it_t>;
};


static_assert(std::is_same<typename BufferSequenceTraits<::boost::asio::mutable_buffer>::iterator,
                           ::boost::asio::mutable_buffer*>::value, "");

static_assert(std::is_same<typename BufferSequenceTraits<::std::vector<::boost::asio::mutable_buffer>>::iterator,
                           ::std::vector<::boost::asio::mutable_buffer>::iterator>::value, "");

template<typename BufferSequence> using SliceSeq =
    ContainerSlice<typename BufferSequenceTraits<BufferSequence>::iterator>;

}  // namespace detail

#ifdef __clang__
  #pragma clang diagnostic push
  #pragma clang diagnostic ignored "-Wmissing-braces"
#endif

template <typename... T>
constexpr auto make_buffer_seq(T&&... values) ->
        std::array<decltype(detail::_MakeCommonBuf(values...)), sizeof...(T)> {
    return {::boost::asio::buffer(values)...};
}

template <size_t N>
constexpr std::array<::boost::asio::mutable_buffer, N + 1>
    make_buffer_seq(const std::array<::boost::asio::mutable_buffer, N>& arr,
                    const ::boost::asio::mutable_buffer& mbuf) {
  std::array<::boost::asio::mutable_buffer, N + 1> res;
  std::copy(arr.begin(), arr.end(), res.begin());
  res[N] = mbuf;
  return res;
}

template <typename It>
std::vector<typename std::iterator_traits<It>::value_type>
    make_buffer_seq(const ContainerSlice<It>& slice,
                    const typename std::iterator_traits<It>::value_type& mbuf) {
  using value_type = typename std::iterator_traits<It>::value_type;
  static_assert(!std::is_const<value_type>::value, "");

  std::vector<value_type> res(slice.size() + 1);
  std::copy(slice.begin(), slice.end(), res.begin());
  res.back() = mbuf;
  return res;
}

template<typename BufferSequence> detail::SliceSeq<BufferSequence>
StripSequence(size_t size, BufferSequence& arg) {
  using Slice = detail::SliceSeq<BufferSequence>;
  using namespace boost::asio;

  auto b = buffer_sequence_begin(arg);
  auto e = buffer_sequence_end(arg);
  if (size == 0) {
    return Slice(b, e);
  }

  for (; b != e; ++b) {
    if (b->size() > size) {
      (*b) += size;  // advance the buffer prefix
      return Slice(b, e);
    }
    size -= b->size();
  }

  return Slice();
}


#ifdef __clang__
#pragma clang diagnostic pop
#endif

}  // namespace util
