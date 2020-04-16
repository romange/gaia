// Copyright 2020, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <liburing/io_uring.h>

#include <boost/asio/detail/buffer_sequence_adapter.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/core/buffers_range.hpp>

#include "absl/base/attributes.h"

namespace util {
namespace uring {

class Proactor;

class FiberSocket {
  FiberSocket(const FiberSocket&) = delete;
  void operator=(const FiberSocket&) = delete;

  explicit FiberSocket(int fd) : fd_(fd) {}

 public:
  using native_handle_type = int;
  using endpoint_type = ::boost::asio::ip::tcp::endpoint;
  using error_code = ::boost::system::error_code;

  FiberSocket() : fd_(-1), p_(nullptr) {
  }

  FiberSocket(FiberSocket&& other) noexcept : fd_(other.fd_), p_(other.p_) {
    other.fd_ = -1;
    other.p_ = nullptr;
  }

  ~FiberSocket();

  FiberSocket& operator=(FiberSocket&& other) noexcept;

  ABSL_MUST_USE_RESULT error_code Listen(unsigned port, unsigned backlog);

  ABSL_MUST_USE_RESULT error_code Accept(FiberSocket* peer);

  ABSL_MUST_USE_RESULT error_code Connect(const endpoint_type& ep);

  ABSL_MUST_USE_RESULT error_code Shutdown(int how);

  ABSL_MUST_USE_RESULT error_code Close();

  // SyncWrite interface:
  // https://www.boost.org/doc/libs/1_69_0/doc/html/boost_asio/reference/SyncWriteStream.html
  template <typename BS> size_t write_some(const BS& bufs, error_code& ec);

  // To calm SyncWriteStream compile-checker we provide exception-enabled interface without
  // implementing it.
  template <typename BS> size_t write_some(const BS& bufs);

  native_handle_type native_handle() const {
    return fd_;
  }

  //! Removes the ownership over file descriptor. Use with caution.
  void Detach() {
    fd_ = -1;
  }

  endpoint_type LocalEndpoint() const;

  //! IsOpen does not promise that the socket is connected or live, just that the file descriptor
  //! is valid.
  bool IsOpen() const {
    return fd_ >= 0;
  }

  void set_proactor(Proactor* p) { p_ = p;}
  Proactor* proactor() { return p_; }

 private:
  size_t Send(const iovec* ptr, size_t len, error_code& ec);

  int fd_;

  // We must reference proactor in each socket so that we could support write_some/read_some
  // with predefined interfance and be compliant with SyncWriteStream/SyncReadStream concepts.
  Proactor* p_;
};

template <typename BS> size_t FiberSocket::write_some(const BS& bufs, error_code& ec) {
  using badapter =
      ::boost::asio::detail::buffer_sequence_adapter<boost::asio::const_buffer, const BS&>;
  badapter bsa(bufs);

  return Send(bsa.buffers(), bsa.count(), ec);
}

}  // namespace uring
}  // namespace util
