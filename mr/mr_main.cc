// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "mr/mr_main.h"

#include "base/init.h"

#include "util/asio/accept_server.h"
#include "util/asio/io_context_pool.h"

namespace mr3 {

DEFINE_int32(http_port, 8080, "Port number.");

using namespace util;

PipelineMain::PipelineMain(int* argc, char*** argv)
  : guard_(new MainInitGuard{argc, argv}), pool_(new IoContextPool) {
  pool_->Run();
  pipeline_.reset(new Pipeline(pool_.get()));

  acc_server_.reset(new AcceptServer(pool_.get()));
  if (FLAGS_http_port >= 0) {
    uint16_t port = acc_server_->AddListener(FLAGS_http_port, &http_listener_);
    LOG(INFO) << "Started http server on port " << port;
  }
  acc_server_->Run();
}

PipelineMain::~PipelineMain() {
  acc_server_->Stop();
  pool_->Stop();
}

}  // namespace mr3
