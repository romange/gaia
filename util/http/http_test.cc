// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "base/gtest.h"
#include "base/logging.h"

#include "util/asio/accept_server.h"
#include "util/asio/io_context_pool.h"
#include "util/http/http_conn_handler.h"

namespace util {

class HttpTest : public testing::Test {
 protected:
  void SetUp() final;

  void TearDown() final;

  std::unique_ptr<AcceptServer> server_;
  std::unique_ptr<IoContextPool> pool_;
  http::Listener<> listener_;
  uint16_t port_ = 0;
};

void HttpTest::SetUp() {
  pool_.reset(new IoContextPool);
  pool_->Run();

  server_.reset(new AcceptServer(pool_.get()));
  port_ = server_->AddListener(0, &listener_);
  server_->Run();
}

void HttpTest::TearDown() {
  LOG(INFO) << "HttpTest::TearDown";

  server_.reset();
  VLOG(1) << "After server reset";
  pool_->Stop();
}

TEST_F(HttpTest, Basic) {
}

}  // namespace util

