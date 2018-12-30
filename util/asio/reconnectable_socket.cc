// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/asio/reconnectable_socket.h"

#include <boost/asio/connect.hpp>
#include <boost/fiber/fiber.hpp>
#include <boost/fiber/operations.hpp>

#include "base/logging.h"

using namespace std;
using chrono::milliseconds;
using chrono::steady_clock;
using namespace boost;
using namespace asio::ip;
using namespace chrono_literals;

namespace util {

namespace detail {

ClientChannelImpl::~ClientChannelImpl() {}

system::error_code ClientChannelImpl::Connect(uint32_t ms) {
  CHECK(!shutting_down_ && shutdown_latch_.IsReady());

  // If we are connected
  if (!status_)
    return status_;

  VLOG(1) << "Connecting on socket " << sock_.native_handle() << " " << sock_.non_blocking()
          << "hostname: " << hostname_ << ", service: " << service_;

  time_point tp = steady_clock::now() + milliseconds(ms);

  // I prefer moving the work to IoContext thread to keep all the data-structure updates
  // single-threaded. It seems that Asio components (timer, for example) in rare cases have
  // data-races. For example, if I call timer.cancel() in one thread but wait on it in another
  // it can ignore "cancel()" call in some cases.
  io_context_.PostFiberSync([this, tp] { ResolveAndConnect(tp); });

  return status_;
}

void ClientChannelImpl::ResolveAndConnect(const time_point& until) {
  auto sleep_dur = 100ms;
  VLOG(1) << "ClientChannel::ResolveAndConnect " << sock_.native_handle();

  asio::steady_timer timer(sock_.get_executor().context());
  fibers_ext::Done done;

  auto connect_cb = [&](const system::error_code& ec, const tcp::endpoint& ep) {
    VLOG(1) << "Async connect with status " << ec << " " << ec.message();
    timer.cancel();

    if (!ec) {
      sock_.non_blocking(true);

      VLOG(1) << "Connected to endpoint " << ep;
    }
    status_ = ec;
    done.Notify();
  };

  while (!shutting_down_ && status_ && steady_clock::now() < until) {
    DCHECK(status_);

    system::error_code resolve_ec;
    auto results = Resolve(fibers_ext::yield[resolve_ec]);
    if (!resolve_ec) {  // OK
      system::error_code ec;
      VLOG(2) << "Connecting to " << results.size() << " endpoints";

      done.Reset();
      timer.expires_at(until);  // Alarm the timer.
      asio::async_connect(sock_, results, connect_cb);

      timer.async_wait(fibers_ext::yield[ec]);
      if (status_ && !ec) {
        // Timer successfully fired, and status_ is still not ok, so cancel async_connect operation.
        sock_.cancel();
      } else if (!status_) {
        VLOG(2) << "Connected, timer status: " << ec.message();
        break;  // connected.
      }
      done.Wait();  // wait for connect_cb to run.
    }

    time_point now = steady_clock::now();
    if (shutting_down_ || now + 2ms >= until) {
      status_ = asio::error::operation_aborted;
      return;
    }

    time_point sleep_until = std::min(now + sleep_dur, until - 2ms);
    timer.expires_at(sleep_until);
    timer.async_wait(fibers_ext::yield[resolve_ec]);
    if (sleep_dur < 1s)
      sleep_dur += 100ms;
  }
// If this code run ReconnectFiber, it became connected DCHECK(!status_);
// However other thread could call a function that would trigger disconnect again.
// DCHECK(!status_) would fail in this case.
//
#if 0
  sock_.async_wait(tcp::socket::wait_read, [this](const boost::system::error_code& ec) {
    system::error_code rec;
    sock_.receive(asio::mutable_buffer(), tcp::socket::message_peek, rec);
    LOG(INFO) << "Got error " << ec.message() << " " << rec << " " << sock_.available();
    //if (!shut)
  });
#endif
}

void ClientChannelImpl::Shutdown() {
  if (!shutting_down_) {
    io_context_.PostFiberSync([this] {
      system::error_code ec;

      shutting_down_ = true;
      VLOG(1) << "Cancelling " << sock_.native_handle();
      sock_.shutdown(tcp::socket::shutdown_both, ec);
      if (!ec) {
        sock_.cancel(ec);
        LOG_IF(INFO, ec) << "Cancelled with status " << ec << " " << ec.message();
      }
      VLOG(1) << "ClientChannelImpl::Shutdown end " << sock_.native_handle() << " " << ec.message();
    });
  }

  shutdown_latch_.Wait();
}

void ClientChannelImpl::ReconnectFiber() {
  this_fiber::properties<IoFiberProperties>().SetNiceLevel(4);

  DCHECK(ReconnectActive());

  while (!shutting_down_ && status_) {
    ResolveAndConnect(steady_clock::now() + 30s);
  }

  shutdown_latch_.Notify();

  if (!status_) {
    VLOG(1) << "Socket " << sock_.native_handle() << " reconnected";
  }
}

void ClientChannelImpl::HandleErrorStatus() {
  if (shutting_down_ || ReconnectActive()) {
    return;
  }
  LOG(INFO) << "Got " << status_.message() << ", reconnecting " << sock_.native_handle();

  shutdown_latch_.Reset();

  io_context_.PostFiber([this] { ReconnectFiber(); });
}

FiberClientSocket::~FiberClientSocket() { Shutdown(); }

void FiberClientSocket::Initiate(const std::string& hname, const std::string& port) {
  CHECK(!worker_.get_id());

  io_context_.PostSynchronous([hname, port, this] {
    worker_ = fibers::fiber(&FiberClientSocket::Worker, this, hname, port);
  });
}

// Waits for socket to become connected. Can be called from any thread.
system::error_code FiberClientSocket::WaitToConnect(uint32_t ms) {
  std::unique_lock<::boost::fibers::mutex> lock(mu_st_);
  cv_st_.wait_for(lock, milliseconds(ms), [this] { return !status_; });
  return status_;
}

// Shuts down all background processes. Can be called from any thread.
void FiberClientSocket::Shutdown() {
  io_context_.PostFiberSync([this] {
    if (!sock_.is_open())
      return;

    sock_.close();
    if (worker_.joinable())
      worker_.join();
  });
}

void FiberClientSocket::Worker(const std::string& hname, const std::string& service) {
  sock_.non_blocking(true);

  while (sock_.is_open()) {
    if (status_) {
      auto ec = Reconnect(hname, service);
      if (ec) {
        VLOG(1) << "Error " << ec << "/" << ec.message();
        this_fiber::sleep_for(10ms);
      }
      continue;
    }
    this_fiber::sleep_for(20ms);
  }
}

system::error_code FiberClientSocket::Reconnect(const std::string& hname,
                                                const std::string& service) {
  auto& asio_io_cntx = io_context_.get_context();
  tcp::resolver resolver(asio_io_cntx);

  system::error_code ec;
  VLOG(2) << "Before AsyncResolve";

  // It seems that resolver waits for 10s and ignores cancel command.
  auto results = resolver.async_resolve(tcp::v4(), hname, service, fibers_ext::yield[ec]);
  if (ec) {
    return ec;
  }

  asio::steady_timer timer(asio_io_cntx, connect_duration_);
  timer.async_wait([&](const system::error_code& ec) {
    if (!ec) {  // Successfully expired.
      VLOG(1) << "Cancelling sock_";
      sock_.cancel();
    }
  });
  asio::async_connect(sock_, results, fibers_ext::yield[status_]);
  if (!status_) {
    cv_st_.notify_all();
  }

  return status_;
}

}  // namespace detail

ReconnectableSocket::~ReconnectableSocket() { Shutdown(); }

}  // namespace util
