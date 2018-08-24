// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "util/asio/channel.h"
#include "util/fibers_done.h"

using std::chrono::milliseconds;

using namespace boost;
using asio::ip::tcp;

namespace util {


system::error_code Channel::Connect(const tcp::endpoint& endpoint, uint32_t ms) {
  fibers_ext::Done done;

  sock_.async_connect(endpoint, [&, this] (system::error_code connect_ec) {
    status_ = connect_ec;
    done.notify();
  });

  asio::steady_timer timer(sock_.get_executor().context(), milliseconds(ms));
  timer.async_wait([&, this](system::error_code timer_ec) {
    // Only if wait succeeded and connect  cb has not been run (ec is ok and is_connecte is false)
    // Only then cancel the socket.
    if (!timer_ec && status_ == asio::error::not_connected) {
      sock_.cancel();
    }
  });

  done.wait();  // switch and wait for callbacks to kick-in

  return status_;
}

}  // namespace util
