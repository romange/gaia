// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "base/init.h"
#include "examples/redis/resp_connection_handler.h"
#include "util/asio/accept_server.h"
#include "util/asio/io_context_pool.h"
#include "util/http/http_conn_handler.h"

using namespace util;
using namespace redis;

DEFINE_int32(http_port, 8080, "Http port.");
DEFINE_int32(port, 6380, "Redis port");

int main(int argc, char** argv) {
  MainInitGuard guard(&argc, &argv);
  IoContextPool pool;
  pool.Run();

  std::unique_ptr<util::AcceptServer> server(new AcceptServer(&pool));
  http::Listener<> http_listener;
  uint16_t port = server->AddListener(FLAGS_http_port, &http_listener);

  LOG(INFO) << "Started http server on port " << port;

  RespListener resp_listener;
  port = server->AddListener(FLAGS_port, &resp_listener);
  LOG(INFO) << "Started redis server on port " << port;
  server->Run();
  server->Wait();

  return 0;
}
