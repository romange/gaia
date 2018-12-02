// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/http/http_client.h"

#include <boost/asio/connect.hpp>
#include <boost/beast/core/flat_buffer.hpp>
#include <boost/beast/http/dynamic_body.hpp>
#include <boost/beast/http/read.hpp>
#include <boost/beast/http/string_body.hpp>
#include <boost/beast/http/write.hpp>

#include "base/logging.h"
#include "util/asio/yield.h"
#include "util/asio/io_context.h"

namespace util {
namespace http {

using fibers_ext::yield;
using namespace detail;

Client::Client(IoContext& io_context) : socket_(io_context.get_context()) {
}

system::error_code Client::Connect(StringPiece host, StringPiece service) {
  tcp::resolver resolver{socket_.get_io_context()};

  system::error_code ec;
  auto results =
      resolver.async_resolve(asio::string_view(host.data(), host.size()),
                             asio::string_view(service.data(), service.size()), yield[ec]);

  if (ec)
    return ec;
  VLOG(1) << "Got " << results.size() << " resolver results";

  asio::async_connect(socket_, results.begin(), results.end(), yield[ec]);

  return ec;
}

system::error_code Client::Get(StringPiece url, Response* response) {
  // Set the URL
  h2::request<h2::string_body> req{h2::verb::get, boost::string_view(url.data(), url.size()), 11};

  // Optional headers
  req.set(h2::field::user_agent, "mytest");

  system::error_code ec;

  // Send the HTTP request to the remote host.
  h2::async_write(socket_, req, yield[ec]);
  if (ec)
    return ec;

  // This buffer is used for reading and must be persisted
  beast::flat_buffer buffer;

  // Receive the HTTP response
  h2::async_read(socket_, buffer, *response, yield[ec]);

  return ec;
}

::boost::system::error_code Client::Cancel() {
  system::error_code ec;
  if (socket_.is_open()) {
    VLOG(1) << "Before shutdown " << socket_.native_handle();
    socket_.cancel(ec);

    socket_.shutdown(detail::tcp::socket::shutdown_both, ec);
    VLOG(1) << "After shutdown: " << ec << " " << ec.message();
  }
  return ec;
}

}  // namespace http
}  // namespace util
