// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include "mr/pipeline.h"
#include "util/http/http_conn_handler.h"

class MainInitGuard;

namespace util {
class AcceptServer;
} // util

namespace mr3 {

class PipelineMain {
public:
  PipelineMain(int* argc, char*** argv);
  ~PipelineMain();

  util::IoContextPool* pool() { return pool_.get(); }
  Pipeline* pipeline() { return pipeline_.get(); }
  util::AcceptServer* accept_server() { return acc_server_.get(); }

private:
  std::unique_ptr<MainInitGuard> guard_;
  std::unique_ptr<util::IoContextPool> pool_;
  std::unique_ptr<Pipeline> pipeline_;
  std::unique_ptr<util::AcceptServer> acc_server_;
  util::http::Listener<> http_listener_;
};

}  // namespace mr3
