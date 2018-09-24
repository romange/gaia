// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include <boost/fiber/fiber.hpp>
#include <boost/asio/connect.hpp>
#include "util/asio/client_channel.h"

#include "base/logging.h"

using namespace std;
using chrono::milliseconds;
using chrono::steady_clock;
using namespace boost;
using namespace asio::ip;

namespace util {

namespace detail {
ClientChannelImpl::~ClientChannelImpl() {
}

system::error_code ClientChannelImpl::Connect(uint32_t ms) {
  CHECK(!shutting_down_ && !reconnect_active_);

  // If we are connected
  if (!status_)
    return status_;

  VLOG(1) << "Connecting on socket " << sock_.native_handle() << " " << sock_.non_blocking();

  CHECK(!hostname_.empty());

  time_point tp = steady_clock::now() + milliseconds(ms);
  ResolveAndConnect(tp);

  return status_;
}

void ClientChannelImpl::ResolveAndConnect(const time_point& until) {
  auto sleep_dur = 100ms;
  VLOG(1) << "ClientChannel::ResolveAndConnect " << sock_.native_handle();

  while (!shutting_down_ && status_ && steady_clock::now() < until) {
    system::error_code resolve_ec;
    auto results = Resolve(fibers_ext::yield[resolve_ec]);
    if (!resolve_ec) {
      system::error_code ec;
      VLOG(2) << "Connecting to " << results.size() << " endpoints";

      tcp::endpoint ep = asio::async_connect(sock_, results, fibers_ext::yield[ec]);
      if (!ec || ec == asio::error::operation_aborted) {
        if (!ec) {
          sock_.non_blocking(true);
          VLOG(1) << "Connected to endpoint " << ep;
        }

        status_ = ec;
        return;
      }
    }
    time_point now = steady_clock::now();
    if (shutting_down_ || now + 2ms >= until) {
      status_ = asio::error::operation_aborted;
      return;
    }

    time_point sleep_until = std::min(now + sleep_dur, until - 2ms);

    asio::steady_timer sleeper(sock_.get_executor().context(), sleep_until);
    sleeper.async_wait(fibers_ext::yield[resolve_ec]);
    if (sleep_dur < 1s)
      sleep_dur += 100ms;
  }
}


void ClientChannelImpl::Shutdown() {
  if (!shutting_down_) {
    system::error_code ec;

    shutting_down_ = true;
    resolver_.cancel();

    VLOG(1) << "Cancelling " << sock_.native_handle();
    sock_.shutdown(tcp::socket::shutdown_both, ec);
    if (!ec) {
      sock_.cancel(ec);
      LOG_IF(INFO, ec) << "Cancelled with status " << ec << " " << ec.message();
    }
    VLOG(1) << "ClientChannelImpl::Shutdown end " << sock_.native_handle() << " " << ec.message();
  }
  std::unique_lock<mutex> l(shd_mu_);
  shd_cnd_.wait(l, [this] { return !reconnect_active_; });
}

void ClientChannelImpl::ReconnectFiber() {
  ResolveAndConnect(steady_clock::now() + 30s);
  DCHECK(reconnect_active_);

  if (!shutting_down_ && status_) {
    ReconnectAsync();  // continue trying.
    return;
  }

  std::lock_guard<mutex> guard(shd_mu_);

  reconnect_active_ = false;

  if (shutting_down_) {
    shd_cnd_.notify_one();
  } else {
    DCHECK(!status_);

    LOG(INFO) << "Socket " << sock_.native_handle() << " reconnected";
  }
}

void ClientChannelImpl::HandleErrorStatus() {
  if (shutting_down_ || reconnect_active_) {
    return;
  }
  LOG(INFO) << "Got " << status_.message() << ", reconnecting " << sock_.native_handle();

  std::lock_guard<mutex> guard(shd_mu_);

  reconnect_active_ = true;

  ReconnectAsync();
}

} // detail


ClientChannel::~ClientChannel() {
  Shutdown();
}

}  // namespace util
