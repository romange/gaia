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

inline int posix_err_wrap(int res, std::error_code* ec) {
  if (res < 0) {
    *ec = std::error_code(errno, std::system_category());
  }
  return res;
}

using namespace boost;

FiberSocket::~FiberSocket() {
  std::error_code ec;
  Close(ec);  // Quietly close.

  LOG_IF(WARNING, ec) << "Error closing socket " << ec;
}

FiberSocket& FiberSocket::operator=(FiberSocket&& other) {
  if (fd_ > 0) {
    std::error_code ec;
    Close(ec);
    LOG_IF(WARNING, ec) << "Error closing socket " << ec;
    fd_ = -1;
  }
  std::swap(fd_, other.fd_);
  return *this;
}

void FiberSocket::Close(std::error_code& ec) {
  if (fd_ > 0) {
    posix_err_wrap(::close(fd_), &ec);
    fd_ = -1;
  }
}

std::error_code FiberSocket::Accept(Proactor* proactor, FiberSocket* peer) {
  sockaddr_in client_addr;
  socklen_t addr_len = sizeof(client_addr);
  fibers::context* me = fibers::context::active();
  std::error_code ec;
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
        Close(ec);
        return std::make_error_code(std::errc::connection_aborted);
      }
      continue;
    }

    posix_err_wrap(res, &ec);
    return ec;
  }
}

}  // namespace uring
}  // namespace util
