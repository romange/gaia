// Copyright 2013, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
// #include "util/http/http_server.h"
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>

#include "util/asio/accept_server.h"
#include "util/asio/io_context_pool.h"
#include "util/asio/yield.h"
#include "util/http/http_server.h"

#include "base/init.h"
#include "absl/strings/str_join.h"
#include "strings/stringpiece.h"

DEFINE_int32(port, 8080, "Port number.");
DEFINE_bool(new_server, true, "");

using namespace std;
using namespace util;
using namespace boost;
using fibers_ext::yield;

namespace http {
  extern string BuildStatusPage();
}

namespace http2 = beast::http;


class HttpHandler : public ConnectionHandler {
 public:
  HttpHandler();

  system::error_code HandleRequest() final override;

 private:
  const char* favicon_;
};


inline absl::string_view as_absl(::boost::string_view s) {
  return absl::string_view(s.data(), s.size());
}

inline system::error_code to_asio(system::error_code ec) {
  if (ec == http2::error::end_of_stream)
    return asio::error::eof;
  return ec;
}

HttpHandler::HttpHandler() {
  favicon_ = "https://rawcdn.githack.com/romange/gaia/master/util/http/logo.png";
}

system::error_code HttpHandler::HandleRequest() {
  beast::flat_buffer buffer;
  http2::request<http2::dynamic_body> request;

  system::error_code ec;

  http2::async_read(*socket_, buffer, request, yield[ec]);
  if (ec) {
    return to_asio(ec);
  }
  http2::response<http2::string_body> response{http2::status::ok, request.version()};
  response.set(http2::field::server, "GAIA");

  if (request.target() == "/favicon.ico") {
    response.set(http2::field::location, favicon_);
    response.result(http2::status::moved_permanently);
  } else {
    if (VLOG_IS_ON(1)) {
      LOG(INFO) << "Target: " << request.target();
      auto formater = [](string* dest, const auto& f) {
        absl::StrAppend(dest, as_absl(f.name_string()), ":", as_absl(f.value()));
      };
      string str = absl::StrJoin(request, ",", formater);
      VLOG(1) << "Fields: " << str;
    }

    response.set(http2::field::content_type, "text/html");
    response.keep_alive(request.keep_alive());
    response.body() = http::BuildStatusPage();
  }
  response.prepare_payload();
  http2::async_write(*socket_, response, yield[ec]);

  return to_asio(ec);
}

int main(int argc, char **argv) {
  MainInitGuard guard(&argc, &argv);

  if (FLAGS_new_server) {
    IoContextPool pool;
    AcceptServer server(&pool);
    pool.Run();

    uint16_t port = server.AddListener(FLAGS_port, [] { return new HttpHandler();});
    LOG(INFO) << "Listening on port " << port;

    server.Run();
    server.Wait();
  } else {
    http::Server server(FLAGS_port);
    util::Status status = server.Start();
    CHECK(status.ok()) << status;
    server.Wait();
  }

  LOG(INFO) << "Exiting server...";
  return 0;
}
