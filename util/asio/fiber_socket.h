// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <boost/asio/ip/tcp.hpp>

#include "util/asio/asio_utils.h"
#include "util/asio/detail/fiber_socket_impl.h"

namespace util {

class IoContext;

class FiberSyncSocket {
 public:
  using error_code = ::boost::system::error_code;
  using next_layer_type = ::boost::asio::ip::tcp::socket;
  using lowest_layer_type = next_layer_type::lowest_layer_type;

  // C'tor can be called from any thread.
  FiberSyncSocket(next_layer_type&& sock, size_t rbuf_size = 1 << 12)
      : impl_(new detail::FiberSocketImpl{std::move(sock), rbuf_size}) {}

  //! Client socket constructor.
  FiberSyncSocket(const std::string& hname, const std::string& port, IoContext* cntx,
                  size_t rbuf_size = 1 << 12)
      : impl_(new detail::FiberSocketImpl{hname, port, cntx, rbuf_size}) {}

  // FiberSyncSocket can not be moveable due to attached fiber.
  FiberSyncSocket(FiberSyncSocket&& other) : impl_(std::move(other.impl_)) {}

  ~FiberSyncSocket() {}

  // Waits for client socket to become connected. Can be called from any thread.
  // Please note that connection status might be stale if called from a foreigh thread.
  error_code ClientWaitToConnect(uint32_t ms) {
    return impl_->ClientWaitToConnect(ms);
  }

  // Read/Write functions should be called from IoContext thread.
  // (fiber) SyncRead interface:
  // https://www.boost.org/doc/libs/1_69_0/doc/html/boost_asio/reference/SyncReadStream.html
  template <typename MBS> size_t read_some(const MBS& bufs, error_code& ec) {
    return impl_->read_some(bufs, ec);
  }

 // To calm SyncReadStream compile-checker we provide exception-enabled interface without
  // implementing it.
  template <typename MBS> size_t read_some(const MBS& bufs);

  // SyncWrite interface:
  // https://www.boost.org/doc/libs/1_69_0/doc/html/boost_asio/reference/SyncWriteStream.html
  template <typename BS> size_t write_some(const BS& bufs, error_code& ec) {
    return impl_->write_some(bufs, ec);
  }

  // To calm SyncWriteStream compile-checker we provide exception-enabled interface without
  // implementing it.
  template <typename BS> size_t write_some(const BS& bufs);

  auto native_handle() { return impl_->native_handle(); }

  bool is_open() const { return impl_ && impl_->is_open(); }

  // Closes the socket and shuts down its background processes if needed.
  // For client socket it's thread-safe but for non-client it should be called
  // from the socket thread.
  void Shutdown(error_code& ec) {
    impl_->Shutdown(ec);
  }

  next_layer_type::endpoint_type remote_endpoint(error_code& ec) const {
    return impl_->remote_endpoint(ec);
  }

  error_code status() const { return impl_->status(); }

  // To support socket requirements.
  next_layer_type& next_layer() { return impl_->next_layer(); }
  lowest_layer_type& lowest_layer() { return impl_->next_layer().lowest_layer(); }

  // For debugging/testing.
  IoContext& context() { return impl_->context(); }

  bool keep_alive() const { return impl_->keep_alive(); }
  void set_keep_alive(bool flag) { impl_->set_keep_alive(flag); }

 private:
  std::unique_ptr<detail::FiberSocketImpl> impl_;
};

static_assert(std::is_move_constructible<FiberSyncSocket>::value, "");

}  // namespace util
