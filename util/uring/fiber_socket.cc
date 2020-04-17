// Copyright 2020, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/uring/fiber_socket.h"

#include <netinet/in.h>
#include <sys/poll.h>

#include <boost/asio/error.hpp>
#include <boost/fiber/context.hpp>

#include "base/logging.h"
#include "util/uring/proactor.h"

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

inline ssize_t posix_err_wrap(ssize_t res, system::error_code* ec) {
  if (res == -1) {
    *ec = system::error_code(errno, system::system_category());
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
  swap(fd_, other.fd_);
  p_ = other.p_;
  other.p_ = nullptr;

  return *this;
}

auto FiberSocket::Shutdown(int how) -> error_code {
  error_code ec;
  posix_err_wrap(::shutdown(fd_, how), &ec);
  return ec;
}

auto FiberSocket::Close() -> error_code {
  error_code ec;
  if (fd_ > 0) {
    posix_err_wrap(::close(fd_), &ec);
    fd_ = -1;
  }
  return ec;
}

auto FiberSocket::Listen(unsigned port, unsigned backlog) -> error_code {
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

  if (posix_err_wrap(
          bind(fd_, (struct sockaddr*)&server_addr, sizeof(server_addr)), &ec) <
      0)
    return ec;

  posix_err_wrap(listen(fd_, backlog), &ec);
  return ec;
}

auto FiberSocket::Accept(FiberSocket* peer) -> error_code {
  CHECK(p_);

  sockaddr_in client_addr;
  socklen_t addr_len = sizeof(client_addr);

  error_code ec;

  while (true) {
    int res = accept4(fd_, (struct sockaddr*)&client_addr, &addr_len,
                      SOCK_NONBLOCK | SOCK_CLOEXEC);
    if (res >= 0) {
      *peer = FiberSocket{res};
      return ec;
    }

    DCHECK_EQ(-1, res);

    if (errno == EAGAIN) {
      FiberCall fc(p_);
      fc->PrepPollAdd(fd_, POLLIN);
      IoResult io_res = fc.Get();

      if (io_res == POLLERR) {
        Close();
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
  CHECK(p_);

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
  posix_err_wrap(::getsockname(fd_, endpoint.data(), &addr_len), &ec);
  CHECK(!ec) << ec << "/" << ec.message() << " while running getsockname";

  endpoint.resize(addr_len);

  return endpoint;
}

size_t FiberSocket::Send(const iovec* ptr, size_t len, error_code& ec) {
  CHECK(p_);
  CHECK_GT(len, 0);

  msghdr msg;
  memset(&msg, 0, sizeof(msg));
  msg.msg_iov = const_cast<iovec*>(ptr);
  msg.msg_iovlen = len;

  ssize_t res;
  while (true) {
    FiberCall fc(p_);
    fc->PrepSendMsg(fd_, &msg, 0);
    res = fc.Get();
    if (res >= 0) {
      break;
    }
    if (res == -ECONNRESET) {
      ec = system::errc::make_error_code(system::errc::connection_aborted);
      break;
    }

    if (res != -EAGAIN) {
      LOG(FATAL) << "Unexpected error " << strerror(-res);
    }
  }
  return res;
}

size_t FiberSocket::Recv(iovec* ptr, size_t len, error_code& ec) {
  CHECK_GT(len, 0);
  CHECK(p_);

  msghdr msg;
  memset(&msg, 0, sizeof(msg));
  msg.msg_iov = const_cast<iovec*>(ptr);
  msg.msg_iovlen = len;

  if (!p_->HasFastPoll()) {
    SubmitEntry se = p_->GetSubmitEntry(nullptr, 0);
    se.PrepPollAdd(fd_, POLLIN);
    se.sqe()->flags = IOSQE_IO_LINK;
  }

  ssize_t res;
  while (true) {
    FiberCall fc(p_);
    fc->PrepRecvMsg(fd_, &msg, 0);
    res = fc.Get();

    if (res >= 0) {  // technically it's eof but we do not have this error here.
      if (res == 0) {
        CHECK(!ec) << ec;  // TBD: To remove.
        ec = system::errc::make_error_code(system::errc::connection_aborted);
      }
      break;
    }

    if (res == -ECONNRESET) {
      ec = system::errc::make_error_code(system::errc::connection_aborted);
      break;
    }

    if (res != -EAGAIN && res != -EBUSY) {
      LOG(FATAL) << "Unexpected error " << strerror(-res);
    }
  }

  return res;
}

}  // namespace uring
}  // namespace util
