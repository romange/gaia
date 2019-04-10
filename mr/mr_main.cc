// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "mr/mr_main.h"

#include "base/init.h"
#include "util/asio/io_context_pool.h"

namespace mr3 {

using namespace util;

PipelineMain::PipelineMain(int* argc, char*** argv)
  : guard_(new MainInitGuard{argc, argv}), pool_(new IoContextPool) {
  pool_->Run();
  pipeline_.reset(new Pipeline(pool_.get()));
}

PipelineMain::~PipelineMain() {
  pool_->Stop();
}

}  // namespace mr3
