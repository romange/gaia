// Copyright 2013, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "util/http/http_server.h"
#include "base/init.h"

DEFINE_int32(port, 8080, "Port number.");

int main(int argc, char **argv) {
  MainInitGuard guard(&argc, &argv);

  http::Server server(FLAGS_port);
  util::Status status = server.Start();
  CHECK(status.ok()) << status;
  server.Wait();
  LOG(INFO) << "Exiting server...";
  return 0;
}

