// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "util/asio/channel.h"
#include "util/fibers_done.h"

using std::chrono::milliseconds;

using namespace boost;
using asio::ip::tcp;

namespace util {


boost::system::error_code Channel::Connect(const tcp::endpoint& endpoint, uint32_t ms) {
  fibers_ext::Done done;

  system::error_code ec;
  sock_.async_connect(endpoint, [&] (system::error_code ec2) {
    ec = ec2;
    if (!ec2)
      is_connected_ = true;
    done.notify();
  });

  asio::steady_timer timer(sock_.get_executor().context(), milliseconds(ms));
  timer.async_wait([&, this](system::error_code timer_ec) {
    // Only if wait succeeded and connect  cb has not been run (ec is ok and is_connecte is false)
    // Only then cancel the socket.
    if (!timer_ec && !ec && !is_connected_) {
      sock_.cancel();
    }
  });

  done.wait();  // switch and wait for callbacks to kick-in

  return ec;
}

}  // namespace util
