// Copyright 2020, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/uring/fiber_socket.h"

#include <netinet/in.h>
#include <sys/poll.h>

#include <boost/fiber/context.hpp>

#include "base/logging.h"
#include "util/uring/proactor.h"

namespace util {
namespace uring {

using namespace std;

inline int posix_err_wrap(int res, error_code* ec) {
  if (res < 0) {
    *ec = error_code(errno, system_category());
  }
  return res;
}

using namespace boost;

FiberSocket::~FiberSocket() {
  error_code ec = Close();  // Quietly close.

  LOG_IF(WARNING, ec) << "Error closing socket " << ec;
}

FiberSocket& FiberSocket::operator=(FiberSocket&& other) {
  if (fd_ > 0) {
    error_code ec = Close();
    LOG_IF(WARNING, ec) << "Error closing socket " << ec;
    fd_ = -1;
  }
  swap(fd_, other.fd_);
  return *this;
}

error_code FiberSocket::Shutdown(int how) {
  error_code ec;
  posix_err_wrap(::shutdown(fd_, how), &ec);
  return ec;
}

error_code FiberSocket::Close() {
  error_code ec;
  if (fd_ > 0) {
    posix_err_wrap(::close(fd_), &ec);
    fd_ = -1;
  }
  return ec;
}

error_code FiberSocket::Listen(unsigned port, unsigned backlog) {
  CHECK_EQ(fd_, -1) << "Close socket before!";

  sockaddr_in server_addr;
  error_code ec;
  fd_ = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
  if (posix_err_wrap(fd_, &ec) < 0)
    return ec;

  const int val = 1;
  setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val));

  memset(&server_addr, 0, sizeof(server_addr));
  server_addr.sin_family = AF_INET;
  server_addr.sin_port = htons(port);
  server_addr.sin_addr.s_addr = INADDR_ANY;

  if (posix_err_wrap(bind(fd_, (struct sockaddr*)&server_addr, sizeof(server_addr)), &ec) < 0)
    return ec;

  posix_err_wrap(listen(fd_, backlog), &ec);
  return ec;
}

error_code FiberSocket::Accept(Proactor* proactor, FiberSocket* peer) {
  sockaddr_in client_addr;
  socklen_t addr_len = sizeof(client_addr);
  fibers::context* me = fibers::context::active();
  error_code ec;
  using IoResult = Proactor::IoResult;

  while (true) {
    int res = accept4(fd_, (struct sockaddr*)&client_addr, &addr_len, SOCK_NONBLOCK | SOCK_CLOEXEC);
    if (res >= 0) {
      *peer = FiberSocket{res};
      return ec;
    }

    DCHECK_EQ(-1, res);

    if (errno == EAGAIN) {
      IoResult io_res = 0;

      auto cb = [me, &io_res](IoResult res, int32_t, Proactor* mgr) {
        io_res = res;
        fibers::context::active()->schedule(me);
      };
      SubmitEntry se = proactor->GetSubmitEntry(std::move(cb), 0);
      se.PrepPollAdd(fd_, POLLIN);
      me->suspend();

      if (io_res == POLLERR) {
        Close();
        return std::make_error_code(std::errc::connection_aborted);
      }
      continue;
    }

    posix_err_wrap(res, &ec);
    return ec;
  }
}

auto FiberSocket::LocalEndpoint() const -> endpoint_type {
  endpoint_type endpoint;

  if (fd_ < 0)
    return endpoint;
  socklen_t addr_len = endpoint.capacity();
  error_code ec;
  posix_err_wrap(::getsockname(fd_, endpoint.data(), &addr_len), &ec);
  CHECK(!ec) << ec << "/" << ec.message() << " while running getsockname";

  endpoint.resize(addr_len);

  return endpoint;
}

}  // namespace uring
}  // namespace util
