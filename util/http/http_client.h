// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/http/dynamic_body.hpp>
#include <boost/beast/http/message.hpp>

#include "strings/stringpiece.h"

namespace util {
class IoContext;

namespace http {

namespace detail {
using namespace boost;
using namespace asio::ip;

namespace h2 = beast::http;
};

class Client {
 public:
  using Response = detail::h2::response<detail::h2::dynamic_body>;

  explicit Client(IoContext& io_context);

  ::boost::system::error_code Connect(StringPiece host, StringPiece service);
  ::boost::system::error_code Get(StringPiece url, Response* response);

 private:
  detail::tcp::socket socket_;
};

}  // namespace http
}  // namespace util

