// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include "mr/pipeline.h"

class MainInitGuard;

namespace mr3 {

class PipelineMain {
public:
  PipelineMain(int* argc, char*** argv);
  ~PipelineMain();

  util::IoContextPool* pool() { return pool_.get(); }
  Pipeline* pipeline() { return pipeline_.get(); }

private:
  std::unique_ptr<MainInitGuard> guard_;
  std::unique_ptr<util::IoContextPool> pool_;
  std::unique_ptr<Pipeline> pipeline_;
};

}  // namespace mr3
