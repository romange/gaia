// Copyright 2020, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <liburing/io_uring.h>

#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/buffer.hpp>

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
  using error_code = std::error_code;

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

  // Really need here expected.
  size_t Send(const iovec* ptr, size_t len, error_code* ec);

  size_t Send(const boost::asio::const_buffer& b, error_code* ec) {
    iovec v{const_cast<void*>(b.data()), b.size()};
    return Send(&v, 1, ec);
  }

  size_t Recv(iovec* ptr, size_t len, error_code* ec);
  size_t Recv(const boost::asio::mutable_buffer& mb, error_code* ec) {
    iovec v{mb.data(), mb.size()};
    return Recv(&v, 1, ec);
  }

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

  static bool IsConnClosed(const error_code& ec) {
    return (ec == std::errc::connection_aborted) || (ec == std::errc::connection_reset);
  }
 private:

  int fd_;

  // We must reference proactor in each socket so that we could support write_some/read_some
  // with predefined interfance and be compliant with SyncWriteStream/SyncReadStream concepts.
  Proactor* p_;
};

}  // namespace uring
}  // namespace util
