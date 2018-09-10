// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "util/http/http_conn_handler.h"

#include <boost/beast/core.hpp>  // for flat_buffer.
#include <boost/beast/http.hpp>

#include "absl/strings/str_join.h"

#include "base/logging.h"
#include "strings/stringpiece.h"
#include "util/asio/yield.h"

using namespace std;

namespace http {
extern string BuildStatusPage();
}

namespace util {
namespace http {
using namespace boost;
namespace http2 = beast::http;

using fibers_ext::yield;

namespace {
inline absl::string_view as_absl(::boost::string_view s) {
  return absl::string_view(s.data(), s.size());
}

inline system::error_code to_asio(system::error_code ec) {
  if (ec == http2::error::end_of_stream)
    return asio::error::eof;
  return ec;
}
}  // namespace

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
  } else if (request.target() == "/") {
    response.set(http2::field::content_type, "text/html");
    response.keep_alive(request.keep_alive());
    response.body() = ::http::BuildStatusPage();
  } else {
    if (VLOG_IS_ON(1)) {
      LOG(INFO) << "Target: " << request.target();
      auto formater = [](string* dest, const auto& f) {
        absl::StrAppend(dest, as_absl(f.name_string()), ":", as_absl(f.value()));
      };
      string str = absl::StrJoin(request, ",", formater);
      VLOG(1) << "Fields: " << str;
    }
  }
  response.prepare_payload();
  http2::async_write(*socket_, response, yield[ec]);

  return to_asio(ec);
}

}  // namespace http
}  // namespace util
