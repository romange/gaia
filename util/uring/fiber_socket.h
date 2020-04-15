// Copyright 2020, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <liburing/io_uring.h>

#include <boost/asio/ip/tcp.hpp>

#include <system_error>
#include "absl/base/attributes.h"

namespace util {
namespace uring {

class Proactor;

class FiberSocket {
  FiberSocket(const FiberSocket&) = delete;
  void operator=(const FiberSocket&) = delete;

 public:
  using native_handle_type = int;
  using endpoint_type = ::boost::asio::ip::tcp::endpoint;

  FiberSocket() : fd_(-1) {
  }

  explicit FiberSocket(int fd) : fd_(fd) {
  }

  FiberSocket(FiberSocket&& other) noexcept : fd_(other.fd_) {
    other.fd_ = -1;
  }

  ~FiberSocket();

  ABSL_MUST_USE_RESULT std::error_code Listen(unsigned port, unsigned backlog);

  ABSL_MUST_USE_RESULT std::error_code Accept(Proactor* proactor, FiberSocket* peer);

  FiberSocket& operator=(FiberSocket&& other);

  ABSL_MUST_USE_RESULT std::error_code Shutdown(int how);

  ABSL_MUST_USE_RESULT std::error_code Close();

  native_handle_type native_handle() const {
    return fd_;
  }

  //! Removes the ownership over file descriptor. Use with caution.
  void Detach() {
    fd_ = -1;
  }

  endpoint_type LocalEndpoint() const;

 private:
  int fd_;
};

}  // namespace uring
}  // namespace util
