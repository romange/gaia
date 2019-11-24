// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <boost/asio/ssl/stream.hpp>
#include "util/asio/fiber_socket.h"

namespace util {
namespace http {

class SslStream {
  using Impl = ::boost::asio::ssl::stream<FiberSyncSocket>;

 public:
  using next_layer_type = Impl::next_layer_type;
  using lowest_layer_type = Impl::lowest_layer_type;
  using error_code = boost::system::error_code;

  SslStream(FiberSyncSocket&& arg, ::boost::asio::ssl::context& ctx);

  // To support socket requirements.
  next_layer_type& next_layer() {
    return next_layer_;
  }

  lowest_layer_type& lowest_layer() {
    return next_layer_.lowest_layer();
  }

  // (fiber) SyncRead interface:
  // https://www.boost.org/doc/libs/1_69_0/doc/html/boost_asio/reference/SyncReadStream.html
  template <typename MBS> size_t read_some(const MBS& bufs, error_code& ec) {
    namespace detail = ::boost::asio::ssl::detail;

    size_t res = detail::io(next_layer_, core_, detail::read_op<MBS>(bufs), ec);
    last_err_ = ec;
    return res;
  }

  //! To calm SyncReadStream compile-checker we provide exception-enabled interface without
  //! implementing it.
  template <typename MBS> size_t read_some(const MBS& bufs);

  //! SyncWrite interface:
  //! https://www.boost.org/doc/libs/1_69_0/doc/html/boost_asio/reference/SyncWriteStream.html
  template <typename BS> size_t write_some(const BS& bufs, error_code& ec) {
    namespace detail = ::boost::asio::ssl::detail;

    size_t res = detail::io(next_layer_, core_, detail::write_op<BS>(bufs), ec);
    last_err_ = ec;
    return res;
  }

  //! To calm SyncWriteStream compile-checker we provide exception-enabled interface without
  //! implementing it.
  template <typename BS> size_t write_some(const BS& bufs);

  void handshake(Impl::handshake_type type, error_code& ec) {
    namespace detail = ::boost::asio::ssl::detail;

    detail::io(next_layer_, core_, detail::handshake_op(type), ec);
  }

  const error_code& last_error() const {
    return last_err_;
  }

  auto native_handle() {
    return core_.engine_.native_handle();
  }

 private:
  FiberSyncSocket next_layer_;
  ::boost::asio::ssl::detail::stream_core core_;
  error_code last_err_;
};


}  // namespace http
}  // namespace util
