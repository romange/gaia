// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "util/asio/fiber_socket.h"

#include <boost/asio/connect.hpp>
#include <chrono>

#include "base/logging.h"
#include "util/asio/io_context.h"

namespace util {

using namespace boost;
using namespace std::chrono_literals;

struct FiberSyncSocket::ClientData {
  ::boost::fibers::fiber worker;
  fibers_ext::condition_variable_any cv_st, worker_cv;
  IoContext* io_cntx;

  fibers::mutex connect_mu;
  ::std::chrono::steady_clock::duration connect_duration = ::std::chrono::seconds(2);

  ClientData(IoContext* io) : io_cntx(io) {}
};

FiberSyncSocket::~FiberSyncSocket() {
  error_code ec;
  Shutdown(ec);
}

FiberSyncSocket::FiberSyncSocket(socket_t&& sock, size_t rbuf_size)
    : rbuf_size_(rbuf_size), sock_(std::move(sock)), rbuf_(new uint8_t[rbuf_size]) {}

// Creates a client socket.
FiberSyncSocket::FiberSyncSocket(const std::string& hname, const std::string& port, IoContext* cntx,
                                 size_t rbuf_size)
    : FiberSyncSocket(socket_t(cntx->raw_context(), asio::ip::tcp::v4()), rbuf_size) {
  status_ = asio::error::not_connected;
  InitiateConnection(hname, port, cntx);
}

void FiberSyncSocket::Shutdown(error_code& ec) {
  if (!is_open_)
    return;
  auto handle = sock_.native_handle();
  auto cb = [&] {
    if (!is_open_) {
      DVLOG(1) << "Already closed " << handle;
      return;
    }

    is_open_ = false;
    sock_.cancel(ec);
    sock_.shutdown(socket_t::shutdown_both, ec);
    VLOG(1) << "Sock Shutdown " << handle;
    if (clientsock_data_) {
      DVLOG(1) << "Sock Closed";
      clientsock_data_->worker_cv.notify_one();
      if (clientsock_data_->worker.joinable())
        clientsock_data_->worker.join();
      DVLOG(1) << "Worker Joined";
    }
  };

  if (clientsock_data_) {
    VLOG(1) << "AwaitShutdown " << handle;
    clientsock_data_->io_cntx->AwaitSafe(cb);
  } else {
    cb();
  }
}

void FiberSyncSocket::WakeWorker() { clientsock_data_->worker_cv.notify_one(); }

void FiberSyncSocket::InitiateConnection(const std::string& hname, const std::string& port,
                                         IoContext* cntx) {
  CHECK(!clientsock_data_ && (&cntx->raw_context() == &sock_.get_executor().context()));

  clientsock_data_.reset(new ClientData(cntx));
  cntx->Await([hname, port, this] {
    rslice_ = asio::buffer(rbuf_.get(), 0);
    clientsock_data_->worker = fibers::fiber(&FiberSyncSocket::Worker, this, hname, port);
  });
}

// Waits for socket to become connected. Can be called from any thread.
system::error_code FiberSyncSocket::ClientWaitToConnect(uint32_t ms) {
  CHECK(clientsock_data_);
  using std::chrono::milliseconds;

  std::unique_lock<fibers::mutex> lock(clientsock_data_->connect_mu);
  clientsock_data_->cv_st.wait_for(lock, milliseconds(ms), [this] { return !status_; });

  return status_;
}

void FiberSyncSocket::Worker(const std::string& hname, const std::string& service) {
  while (is_open_) {
    if (status_) {
      error_code ec = Reconnect(hname, service);
      VLOG(1) << "After  Reconnect: " << ec << "/" << ec.message() << " is_open: " << is_open_;
      if (ec && is_open_) {  // Only sleep for open socket for the next reconnect.
        this_fiber::sleep_for(10ms);
      }
      continue;
    }
    DCHECK(sock_.non_blocking());

    error_code ec;
    VLOG(2) << "BeforeAsyncWait";
    sock_.async_wait(socket_t::wait_read, fibers_ext::yield[ec]);
    if (ec) {
      LOG_IF(ERROR, is_open_) << "AsyncWait: " << ec.message();
      continue;
    }

    size_t read_capacity = rbuf_size_ - rslice_.size();
    if (read_state_ == READ_IDLE && read_capacity) {
      uint8_t* next = static_cast<uint8_t*>(rslice_.data()) + rslice_.size();
      // Direct but non-blocking call since we know we should be able to receive.
      // Since it's direct - we do not context-switch.
      size_t read_cnt = sock_.receive(asio::mutable_buffer(next, read_capacity), 0, status_);
      if (status_) {
        VLOG(1) << "SockReceive: " << status_.message();
      } else {
        rslice_ = asio::mutable_buffer(rslice_.data(), rslice_.size() + read_cnt);
      }
      continue;
    }
    VLOG(2) << "BeforeCvReadWait";
    auto should_iterate = [this] {
      return !is_open() || (read_state_ == READ_IDLE && rslice_.size() != rbuf_size_);
    };

    fibers::mutex mu;
    std::unique_lock<fibers::mutex> lock(mu);
    clientsock_data_->worker_cv.wait(lock, should_iterate);

    VLOG(2) << "WorkerIteration: ";
  }
  VLOG(1) << "FiberSocketReadExit";
}

system::error_code FiberSyncSocket::Reconnect(const std::string& hname,
                                              const std::string& service) {
  DCHECK(clientsock_data_);
  using namespace asio::ip;

  auto& asio_io_cntx = clientsock_data_->io_cntx->raw_context();
//  asio::steady_timer timer(asio_io_cntx, clientsock_data_->connect_duration);

  tcp::resolver resolver(asio_io_cntx);

  /*timer.async_wait([&](const system::error_code& ec) {
    if (!ec) {  // Successfully expired.
      VLOG(1) << "Cancelling resolver";
      resolver.cancel();
    }
  });*/

  system::error_code ec;
  VLOG(1) << "Before AsyncResolve";

  // It seems that resolver waits for 10s and ignores cancel command.
  auto results = resolver.async_resolve(tcp::v4(), hname, service, fibers_ext::yield[ec]);
  if (ec) {
    return ec;
  }
  DVLOG(1) << "After AsyncResolve";

  asio::steady_timer timer(asio_io_cntx, clientsock_data_->connect_duration);
  timer.async_wait([&](const system::error_code& ec) {
    if (!ec) {  // Successfully expired.
      VLOG(1) << "Cancelling sock_";
      sock_.cancel();
    }
  });

  asio::async_connect(sock_, results, fibers_ext::yield[ec]);
  DVLOG(1) << "After async_connect " << ec;

  if (!ec) {
    sock_.non_blocking(true);  // For some reason async_connect clears this option.

    // Use mutex to so that WaitToConnect would be thread-safe.
    std::lock_guard<fibers::mutex> lock(clientsock_data_->connect_mu);
    status_ = ec;
    // notify_one awakes only those threads that already suspend on cnd.wait(). Therefore
    // we must change status_ under mutex.
    clientsock_data_->cv_st.notify_one();
  }
  status_ = ec;

  return status_;
}

IoContext& FiberSyncSocket::context() {
  CHECK(clientsock_data_);
  return *clientsock_data_->io_cntx;
}

}  // namespace util
