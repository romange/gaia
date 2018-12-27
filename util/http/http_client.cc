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
#include "util/asio/reconnectable_socket.h"
#include "util/asio/yield.h"

namespace util {
namespace http {

using fibers_ext::yield;
using namespace boost;

namespace h2 = beast::http;

Client::Client(IoContext* io_context) : io_context_(*io_context) {}

Client::~Client() {}

system::error_code Client::Connect(StringPiece host, StringPiece service) {
  socket_.reset(
      new ReconnectableSocket(strings::AsString(host), strings::AsString(service), &io_context_));

  return socket_->Connect(connect_timeout_ms_);
}


::boost::system::error_code Client::Get(StringPiece url, StringPiece body, Response* response) {
  // Set the URL
  h2::request<h2::string_body> req{h2::verb::get, boost::string_view(url.data(), url.size()), 11};

  // Optional headers
  for (const auto& k_v : headers_) {
    req.set(k_v.first, k_v.second);
  }
  req.body().assign(body.begin(), body.end());

  // Send the HTTP request to the remote host.
  system::error_code ec = socket_->Apply([&] (ReconnectableSocket::socket_t& s) {
    system::error_code ec;
    h2::async_write(s, req, yield[ec]);
    return ec;
  });

  VLOG(2) << "Req: " << req;
  if (ec) {
    VLOG(1) << "Error " << ec;
    return ec;
  }

  // Receive the HTTP response
  ec = socket_->Apply([&] (ReconnectableSocket::socket_t& s) {
    system::error_code ec;
    // This buffer is used for reading and must be persisted
    beast::flat_buffer buffer;

    h2::async_read(s, buffer, *response, yield[ec]);
    return ec;
  });

  return ec;
}

void Client::Shutdown() {
  if (socket_) {
    socket_->Shutdown();
    socket_.reset();
  }
}

bool Client::IsConnected() const {
  return socket_ && !socket_->is_shut_down() && !socket_->status();
}

}  // namespace http
}  // namespace util
