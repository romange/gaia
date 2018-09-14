// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include <boost/fiber/fiber.hpp>

#include "util/asio/channel.h"

#include "base/logging.h"

using namespace std;
using chrono::milliseconds;
using chrono::steady_clock;

using namespace boost;

namespace util {

namespace detail {
ClientChannelImpl::~ClientChannelImpl() {
}

system::error_code ClientChannelImpl::Connect(uint32_t ms) {
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
      for (auto& ep : results) {
        sock_.async_connect(ep, fibers_ext::yield[status_]);
        if (!status_ || status_ == asio::error::operation_aborted)
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
    shutting_down_ = true;
    sock_.cancel();
    resolver_.cancel();

    system::error_code ec;
    sock_.shutdown(tcp::socket::shutdown_both, ec);
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
