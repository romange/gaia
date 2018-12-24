// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

// #include <boost/asio/ip/tcp.hpp>
#include <boost/beast/http/dynamic_body.hpp>
#include <boost/beast/http/message.hpp>

#include "strings/stringpiece.h"

namespace util {
class IoContext;
class ReconnectableSocket;

namespace http {

/*
  Single threaded, fiber-friendly synchronous client: Upon IO block, the calling fiber blocks
  but the thread can switch to other active fibers.
*/
class Client {
 public:
  using Response = boost::beast::http::response<boost::beast::http::dynamic_body>;

  explicit Client(IoContext* io_context);
  ~Client();

  ::boost::system::error_code Connect(StringPiece host, StringPiece service);
  ::boost::system::error_code Get(StringPiece url, Response* response);

  void Shutdown();

  bool IsConnected() const;

  void set_connect_timeout_ms(uint32_t ms) { connect_timeout_ms_ = ms; }
 private:
  IoContext& io_context_;
  uint32_t connect_timeout_ms_ = 2000;

  std::unique_ptr<ReconnectableSocket> socket_;
};

}  // namespace http
}  // namespace util
