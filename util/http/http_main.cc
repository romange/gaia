// Copyright 2013, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/http/status.hpp>

#include "util/asio/accept_server.h"
#include "util/asio/io_context_pool.h"

#include "util/asio/yield.h"
#include "util/http/http_conn_handler.h"

#include "absl/strings/str_join.h"
#include "base/init.h"
#include "strings/stringpiece.h"

DEFINE_int32(port, 8080, "Port number.");

using namespace std;
using namespace boost;
using namespace util;
namespace h2 = beast::http;

int main(int argc, char** argv) {
  MainInitGuard guard(&argc, &argv);

  IoContextPool pool;
  AcceptServer server(&pool);
  pool.Run();
  http::Listener<> listener;
  auto cb = [](const http::QueryArgs& args, http::HttpHandler::SendFunction* send) {
    http::StringResponse resp = http::MakeStringResponse(h2::status::ok);
    resp.body() = "Bar";
    return send->Invoke(std::move(resp));
  };

  listener.RegisterCb("/foo", false, cb);
  uint16_t port = server.AddListener(FLAGS_port, &listener);
  LOG(INFO) << "Listening on port " << port;

  server.Run();
  server.Wait();

  LOG(INFO) << "Exiting server...";
  return 0;
}
