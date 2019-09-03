// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "util/asio/fiber_socket.h"

#include <boost/asio/connect.hpp>
#include <chrono>

#include "absl/base/attributes.h"
#include "base/logging.h"
#include "util/asio/io_context.h"

#define VSOCK(verbosity) VLOG(verbosity) << "sock[" << native_handle() << "] "
#define DVSOCK(verbosity) DVLOG(verbosity) << "sock[" << native_handle() << "] "

namespace util {

using namespace boost;
using namespace std;
using namespace chrono_literals;

namespace detail {

template <typename Duration> uint32_t ms_duration(const Duration& d) {
  return chrono::duration_cast<chrono::milliseconds>(d).count();
}

using socket_t = FiberSocketImpl::next_layer_type;

struct FiberSocketImpl::ClientData {
  ::boost::fibers::fiber worker;
  fibers_ext::condition_variable_any cv_st, worker_cv;
  IoContext* io_cntx;

  fibers::mutex connect_mu;
  chrono::steady_clock::duration connect_duration = chrono::seconds(2);

  ClientData(IoContext* io) : io_cntx(io) {}
};

FiberSocketImpl::~FiberSocketImpl() {
  VLOG(1) << "FiberSocketImpl::~FiberSocketImpl";

  error_code ec;
  Shutdown(ec);
}

FiberSocketImpl::FiberSocketImpl(socket_t&& sock, size_t rbuf_size)
    : rbuf_size_(rbuf_size), sock_(std::move(sock)), rbuf_(new uint8_t[rbuf_size]) {}

// Creates a client socket.
FiberSocketImpl::FiberSocketImpl(const std::string& hname, const std::string& port, IoContext* cntx,
                                 size_t rbuf_size)
    : FiberSocketImpl(socket_t(cntx->raw_context()), rbuf_size) {
  VLOG(1) << "FiberSocketImpl::FiberSocketImpl " << sock_.native_handle();
  hname_ = hname;
  port_ = port;
  io_cntx_ = cntx;
  status_ = asio::error::not_connected;
}

void FiberSocketImpl::Shutdown(error_code& ec) {
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
    VSOCK(1) << "Sock Shutdown ";
    if (clientsock_data_) {
      DVSOCK(1) << "Sock Closed";
      clientsock_data_->worker_cv.notify_one();
      if (clientsock_data_->worker.joinable())
        clientsock_data_->worker.join();
      DVSOCK(1) << "Worker Joined";
    }
  };

  if (clientsock_data_) {
    VSOCK(1) << "AwaitShutdown";
    clientsock_data_->io_cntx->AwaitSafe(cb);
  } else {
    cb();
  }
}

void FiberSocketImpl::SetStatus(const error_code& ec, const char* where) {
  status_ = ec;
  if (ec) {
    VLOG(1) << "Setting socket status to " << ec << "/" << ec.message() << " at " << where;
  }
}

void FiberSocketImpl::WakeWorker() { clientsock_data_->worker_cv.notify_one(); }

void FiberSocketImpl::InitiateConnection() {
  CHECK(!clientsock_data_ && (&io_cntx_->raw_context() == &sock_.get_executor().context()));

  clientsock_data_.reset(new ClientData(io_cntx_));
  io_cntx_->Await([this] {
    rslice_ = asio::buffer(rbuf_.get(), 0);
    clientsock_data_->worker = fibers::fiber(&FiberSocketImpl::ClientWorker, this);
  });
}

// Waits for socket to become connected. Can be called from any thread.
system::error_code FiberSocketImpl::ClientWaitToConnect(uint32_t ms) {
  if (!clientsock_data_) {
    InitiateConnection();
  }
  using std::chrono::milliseconds;

  std::unique_lock<fibers::mutex> lock(clientsock_data_->connect_mu);
  clientsock_data_->cv_st.wait_for(lock, milliseconds(ms), [this] { return !status_; });

  return status_;
}

void FiberSocketImpl::ClientWorker() {
  while (is_open_) {
    if (status_) {
      VSOCK(1) << "Status " << status_;
      error_code ec = Reconnect(hname_, port_);
      VSOCK(1) << "After  Reconnect: " << ec << "/" << ec.message() << " is_open: " << is_open_;
      if (ec && is_open_) {  // Only sleep for open socket for the next reconnect.
        this_fiber::sleep_for(10ms);
      }
      continue;
    }
    DCHECK(sock_.non_blocking());

    error_code ec;
    VSOCK(3) << "BeforeAsyncWait";
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
      if (!status_) {
        rslice_ = asio::mutable_buffer(rslice_.data(), rslice_.size() + read_cnt);
      } else if (status_ == system::errc::resource_unavailable_try_again) {
        status_.clear();
      } else {
        VSOCK(1) << "Receive: " << status_.message() << ", read_cnt " << 0;
      }
      continue;
    }
    VLOG(3) << "BeforeCvReadWait";
    auto should_iterate = [this] {
      return !is_open() || (read_state_ == READ_IDLE && rslice_.size() != rbuf_size_);
    };

    fibers::mutex mu;
    std::unique_lock<fibers::mutex> lock(mu);
    clientsock_data_->worker_cv.wait(lock, should_iterate);

    VLOG(3) << "WorkerIteration: ";
  }
  VLOG(1) << "FiberSocketReadExit";
}

ABSL_MUST_USE_RESULT system::error_code
OpenAndConfigure(bool keep_alive, asio::ip::tcp::socket& sock) {
  using namespace asio::ip;
  system::error_code ec;

  if (sock.is_open()) {
    sock.close(ec);
    CHECK(!sock.is_open());
  }
  sock.open(tcp::v4(), ec);
  if (ec)
    return ec;
  CHECK(sock.is_open());

  // We close before we configure. That way we can reuse the same socket descriptor.
  socket_t::reuse_address opt(true);
  sock.set_option(opt, ec);
  if (ec)
    return ec;
  socket_t::keep_alive opt2(keep_alive);
  sock.set_option(opt2, ec);
  if (ec)
    return ec;

  sock.non_blocking(true, ec);
  if (ec)
    return ec;
  return system::error_code{};
}

system::error_code FiberSocketImpl::Reconnect(const std::string& hname,
                                              const std::string& service) {
  DCHECK(clientsock_data_);
  using namespace asio::ip;

  auto& asio_io_cntx = clientsock_data_->io_cntx->raw_context();

  tcp::resolver resolver(asio_io_cntx);

  system::error_code ec;

  VSOCK(1) << "Before AsyncResolve " << is_open();

  // It seems that resolver waits for 10s and ignores cancel command.
  auto results = resolver.async_resolve(tcp::v4(), hname, service, fibers_ext::yield[ec]);
  if (ec) {
    VLOG(1) << "Resolver error " << ec;
    return ec;
  }
  DVSOCK(1) << "After AsyncResolve, got " << results.size() << " results";

  asio::steady_timer timer(asio_io_cntx, clientsock_data_->connect_duration);
  timer.async_wait([&](const system::error_code& ec) {
    if (!ec) {  // Successfully expired.
      VSOCK(1) << "Cancelling after "
              << detail::ms_duration(clientsock_data_->connect_duration) << " millis.";
      sock_.cancel();
    }
  });

  //! I do not use asio::async_connect because it closes the socket underneath and looses
  //! all the configuration and options we set before connect.
  size_t result_index = 0;
  for (const auto& remote_dest : results) {
    ec = OpenAndConfigure(keep_alive_, sock_);
    if (ec)
      break;
    sock_.async_connect(remote_dest, fibers_ext::yield[ec]);

    // If we succeeded - break the loop.
    // Also, for operation aborted we do not iterate since it means we went over connect_duration limit.
    if (!ec || ec == asio::error::operation_aborted)
      break;
    VSOCK(2) << "Connect iteration " << result_index << ", error " << ec;
    ++result_index;
  }
  VSOCK(1) << "After async_connect " << ec << " result index " << result_index;

  if (ec) {
    SetStatus(ec, "reconnect");
  } else {
    CHECK(sock_.non_blocking()) << native_handle();

    socket_t::keep_alive ka_opt;
    sock_.get_option(ka_opt, ec);
    CHECK_EQ(keep_alive_, ka_opt.value()) << keep_alive_;

    // Use mutex to so that WaitToConnect would be thread-safe.
    std::lock_guard<fibers::mutex> lock(clientsock_data_->connect_mu);
    status_.clear();

    // notify_one awakes only those threads that already suspend on cnd.wait(). Therefore
    // we must change status_ under mutex.
    clientsock_data_->cv_st.notify_one();
  }
  return status_;
}

IoContext& FiberSocketImpl::context() {
  CHECK(clientsock_data_);
  return *clientsock_data_->io_cntx;
}

}  // namespace detail
}  // namespace util
