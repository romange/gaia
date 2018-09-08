// Copyright 2013, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
// #include "util/http/http_server.h"
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>

#include "util/asio/accept_server.h"
#include "util/asio/io_context_pool.h"
#include "util/asio/yield.h"

#include "base/init.h"

DEFINE_int32(port, 8080, "Port number.");

using namespace util;
using namespace boost;
using fibers_ext::yield;

namespace http = beast::http;

class HttpHandler : public ConnectionHandler {
 public:
  HttpHandler() {}

  system::error_code HandleRequest() final override;

 private:
};

system::error_code HttpHandler::HandleRequest() {
  beast::flat_buffer buffer{8192};
  http::request<http::dynamic_body> request;

  system::error_code ec;

  http::async_read(*socket_, buffer, request, yield[ec]);
  if (ec)
    return ec;

  http::response<http::string_body> response{http::status::ok, request.version()};
  response.set(http::field::server, "GAIA");
  response.set(http::field::content_type, "text/plain");
  response.keep_alive(request.keep_alive());
  response.body() = "TEST";
  response.prepare_payload();
  http::async_write(*socket_, response, yield[ec]);

  return ec;
}

int main(int argc, char **argv) {
  MainInitGuard guard(&argc, &argv);

  IoContextPool pool;
  AcceptServer server(&pool);
  pool.Run();

  uint16_t port = server.AddListener(FLAGS_port, [] { return new HttpHandler();});
  LOG(INFO) << "Listening on port " << port;

  server.Run();
  server.Wait();

  /*http::Server server(FLAGS_port);
  util::Status status = server.Start();
  CHECK(status.ok()) << status;
  server.Wait();*/
  

  LOG(INFO) << "Exiting server...";
  return 0;
}
