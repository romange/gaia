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

class LocalRunner;

class PipelineMain {
public:
  //! This sets up MR pipeline object and all its dependencies like IO pool.
  PipelineMain();

  //! This c'tor runs MainInitGuard as part of its initialization flow and then
  // everything that PipelineMain() does.
  PipelineMain(int* argc, char*** argv);

  ~PipelineMain();

  util::IoContextPool* pool() { return pool_.get(); }
  Pipeline* pipeline() { return pipeline_.get(); }
  util::AcceptServer* accept_server() { return acc_server_.get(); }

  LocalRunner* StartLocalRunner(const std::string& root_dir, bool stop_on_break = true);

private:
  void Init();

  std::unique_ptr<MainInitGuard> guard_;  // Must be first to be destructed last.

  std::unique_ptr<util::IoContextPool> pool_;
  std::unique_ptr<Pipeline> pipeline_;
  std::unique_ptr<util::AcceptServer> acc_server_;
  util::http::Listener<> http_listener_;
  std::unique_ptr<LocalRunner> runner_;
};

}  // namespace mr3
