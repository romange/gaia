// Copyright 2020, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/uring/fiber_socket.h"

#include <netinet/in.h>
#include <sys/poll.h>

#include <boost/fiber/context.hpp>

#include "base/logging.h"
#include "base/stl_util.h"
#include "util/uring/proactor.h"

#define VSOCK(verbosity) VLOG(verbosity) << "sock[" << native_handle() << "] "
#define DVSOCK(verbosity) DVLOG(verbosity) << "sock[" << native_handle() << "] "

namespace util {
namespace uring {

using namespace std;
using namespace boost;
using IoResult = Proactor::IoResult;

namespace {

class FiberCall {
  SubmitEntry se_;
  fibers::context* me_;
  IoResult io_res_;

 public:
  FiberCall(Proactor* proactor) : me_(fibers::context::active()), io_res_(0) {
    auto waker = [this](IoResult res, int32_t, Proactor* mgr) {
      io_res_ = res;
      fibers::context::active()->schedule(me_);
    };
    se_ = proactor->GetSubmitEntry(std::move(waker), 0);
  }

  ~FiberCall() {
    CHECK(!me_) << "Get was not called!";
  }

  SubmitEntry* operator->() {
    return &se_;
  }

  IoResult Get() {
    me_->suspend();
    me_ = nullptr;

    return io_res_;
  }
};

inline ssize_t posix_err_wrap(ssize_t res, FiberSocket::error_code* ec) {
  if (res == -1) {
    *ec = FiberSocket::error_code(errno, std::system_category());
  } else if (res < 0) {
    LOG(WARNING) << "Bad posix error " << res;
  }
  return res;
}

}  // namespace

FiberSocket::~FiberSocket() {
  error_code ec = Close();  // Quietly close.

  LOG_IF(WARNING, ec) << "Error closing socket " << ec << "/" << ec.message();
}

FiberSocket& FiberSocket::operator=(FiberSocket&& other) noexcept {
  if (fd_ >= 0) {
    error_code ec = Close();
    LOG_IF(WARNING, ec) << "Error closing socket " << ec << "/" << ec.message();
  }
  DCHECK_EQ(-1, fd_);

  swap(fd_, other.fd_);
  p_ = other.p_;
  other.p_ = nullptr;

  return *this;
}

auto FiberSocket::Shutdown(int how) -> error_code {
  CHECK_GE(fd_, 0);

  // If we shutdown and then try to Send/Recv - the call will stall since no data
  // is sent/received. Therefore we remember the state to allow consistent API experience.
  error_code ec;
  if (fd_ & IS_SHUTDOWN)
    return ec;

  posix_err_wrap(::shutdown(fd_ & FD_MASK, how), &ec);
  fd_ |= IS_SHUTDOWN;  // Enter shutdown state unrelated to the success of the call.

  return ec;
}

auto FiberSocket::Close() -> error_code {
  error_code ec;
  if (fd_ > 0) {
    DVSOCK(1) << "Closing socket";

    posix_err_wrap(::close(fd_ & FD_MASK), &ec);
    fd_ = -1;
  }
  return ec;
}

auto FiberSocket::Listen(unsigned port, unsigned backlog, uint32_t sock_opts_mask) -> error_code {
  CHECK_EQ(fd_, -1) << "Close socket before!";

  error_code ec;
  fd_ = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
  if (posix_err_wrap(fd_, &ec) < 0)
    return ec;

  const int val = 1;
  for (int opt = 0; sock_opts_mask; ++opt) {
    if (sock_opts_mask & 1) {
      if (setsockopt(fd_, SOL_SOCKET, opt, &val, sizeof(val)) < 0) {
        LOG(WARNING) << "setsockopt: could not set opt " << opt << ", " << strerror(errno);
      }
    }
    sock_opts_mask >>= 1;
  }

  sockaddr_in server_addr;
  memset(&server_addr, 0, sizeof(server_addr));
  server_addr.sin_family = AF_INET;
  server_addr.sin_port = htons(port);
  server_addr.sin_addr.s_addr = INADDR_ANY;

  if (posix_err_wrap(bind(fd_, (struct sockaddr*)&server_addr, sizeof(server_addr)), &ec) < 0)
    return ec;

  posix_err_wrap(listen(fd_, backlog), &ec);
  return ec;
}

auto FiberSocket::Accept(FiberSocket* peer) -> error_code {
  CHECK(p_);

  sockaddr_in client_addr;
  socklen_t addr_len = sizeof(client_addr);

  error_code ec;
  int fd = fd_ & FD_MASK;

  while (true) {
    int res = accept4(fd, (struct sockaddr*)&client_addr, &addr_len, SOCK_NONBLOCK | SOCK_CLOEXEC);
    if (res >= 0) {
      *peer = FiberSocket{res};
      return ec;
    }

    DCHECK_EQ(-1, res);

    if (errno == EAGAIN) {
      FiberCall fc(p_);
      fc->PrepPollAdd(fd, POLLIN);
      IoResult io_res = fc.Get();

      if (io_res == POLLERR) {
        return system::errc::make_error_code(system::errc::connection_aborted);
      }
      continue;
    }

    posix_err_wrap(res, &ec);
    return ec;
  }
}

auto FiberSocket::Connect(const endpoint_type& ep) -> error_code {
  CHECK_EQ(fd_, -1);
  CHECK(p_ && p_->InMyThread());

  error_code ec;

  fd_ = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (posix_err_wrap(fd_, &ec) < 0)
    return ec;

  FiberCall fc(p_);
  fc->PrepConnect(fd_, ep.data(), ep.size());

  IoResult io_res = fc.Get();
  if (io_res < 0) {  // In that case connect returns -errno.
    if (close(fd_) < 0) {
      LOG(WARNING) << "Could not close fd " << strerror(errno);
    }
    fd_ = -1;
    ec = error_code(-io_res, system::system_category());
  }
  return ec;
}

auto FiberSocket::LocalEndpoint() const -> endpoint_type {
  endpoint_type endpoint;

  if (fd_ < 0)
    return endpoint;
  socklen_t addr_len = endpoint.capacity();
  error_code ec;
  posix_err_wrap(::getsockname(fd_ & FD_MASK, endpoint.data(), &addr_len), &ec);
  CHECK(!ec) << ec << "/" << ec.message() << " while running getsockname";

  endpoint.resize(addr_len);

  return endpoint;
}

auto FiberSocket::RemoteEndpoint() const -> endpoint_type {
  endpoint_type endpoint;
  CHECK_GT(fd_, 0);

  socklen_t addr_len = endpoint.capacity();
  error_code ec;
  if (getpeername(fd_ & FD_MASK, endpoint.data(), &addr_len) == 0)
    endpoint.resize(addr_len);

  return endpoint;
}

auto FiberSocket::Send(const iovec* ptr, size_t len) -> expected_size_t {
  CHECK(p_);
  CHECK_GT(len, 0);
  CHECK_GE(fd_, 0);

  if (fd_ & IS_SHUTDOWN) {
    return nonstd::make_unexpected(std::make_error_code(std::errc::connection_aborted));
  }

  msghdr msg;
  memset(&msg, 0, sizeof(msg));
  msg.msg_iov = const_cast<iovec*>(ptr);
  msg.msg_iovlen = len;

  ssize_t res;
  int fd = fd_ & FD_MASK;

  while (true) {
    FiberCall fc(p_);
    fc->PrepSendMsg(fd, &msg, 0);
    res = fc.Get();  // Interrupt point
    if (res >= 0) {
      return res;  // Fastpath
    }
    DVSOCK(1) << "Got " << res;
    res = -res;
    if (res == EAGAIN || res == EBUSY)
      continue;

    if (base::_in(res, {ECONNABORTED, EPIPE, ECONNRESET})) {
      if (res == EPIPE)  // We do not care about EPIPE that can happen when we shutdown our socket.
        res = ECONNABORTED;
      break;
    }

    LOG(FATAL) << "Unexpected error " << res << "/" << strerror(res);
  }
  std::error_code ec(res, std::generic_category());
  VSOCK(1) << "Error " << ec << " on " << RemoteEndpoint();

  return nonstd::make_unexpected(std::move(ec));
}

auto FiberSocket::Recv(iovec* ptr, size_t len) -> expected_size_t {
  CHECK_GT(len, 0);
  CHECK(p_);
  CHECK_GE(fd_, 0);

  if (fd_ & IS_SHUTDOWN) {
    return nonstd::make_unexpected(std::make_error_code(std::errc::connection_aborted));
  }

  msghdr msg;
  memset(&msg, 0, sizeof(msg));
  msg.msg_iov = const_cast<iovec*>(ptr);
  msg.msg_iovlen = len;
  int fd = fd_ & FD_MASK;

  // There is a possible data-race bug since GetSubmitEntry can preempt inside
  // FiberCall, thus introducing a chain with random SQE not from here.
  //
  // The bug is not really interesting in this context here since we handle the use-case of old
  // kernels without fast-poll, however it's problematic for transactions that require SQE chains.
  // Added TODO to proactor.h
  if (!p_->HasFastPoll()) {
    SubmitEntry se = p_->GetSubmitEntry(nullptr, 0);
    se.PrepPollAdd(fd, POLLIN);
    se.sqe()->flags = IOSQE_IO_LINK;
  }

  ssize_t res;
  while (true) {
    FiberCall fc(p_);
    fc->PrepRecvMsg(fd, &msg, 0);
    res = fc.Get();

    if (res > 0) {
      return res;
    }
    DVSOCK(1) << "Got " << res;

    res = -res;
    if (res == EAGAIN || res == EBUSY)
      continue;

    if (res == 0)
      res = ECONNABORTED;

    if (base::_in(res, {ECONNABORTED, EPIPE, ECONNRESET})) {
      break;
    }

    LOG(FATAL) << "Unexpected error " << res << "/" << strerror(res);
  }
  std::error_code ec(res, std::generic_category());
  VSOCK(1) << "Error " << ec << " on " << RemoteEndpoint();
  expected_size_t es;
  es.operator bool();

  return nonstd::make_unexpected(std::move(ec));
}

}  // namespace uring
}  // namespace util
