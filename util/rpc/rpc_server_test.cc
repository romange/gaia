// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include <chrono>
#include <memory>

#include "base/gtest.h"
#include "base/logging.h"
#include "base/walltime.h"

#include "util/asio/io_context_pool.h"
#include "util/rpc/rpc_server.h"

namespace util {

class RpcTestBridge final : public RpcConnectionBridge {
 public:
  // header and letter are input/output parameters.
  // HandleEnvelope reads first the input and if everything is parsed fine, it sends
  // back another header, letter pair.
  Status HandleEnvelope(base::PODArray<uint8_t>* header,
                        base::PODArray<uint8_t>* letter) override {
    return Status::OK;
  }
};

class RpcTestInterface final : public RpcServiceInterface {
 public:
  RpcConnectionBridge* CreateConnectionBridge() override { return new RpcTestBridge{}; }
};

class RpcServerTest : public testing::Test {
protected:
  static void SetUpTestCase() {
    rpc_server_.reset(new RpcServer(0));
    pool_.reset(new IoContextPool);
  }

  static void TearDownTestCase() {
    rpc_server_.reset();
  }

  void SetUp() override {
    service_.reset(new RpcTestInterface);
  }

  void TearDown() override {}

  std::unique_ptr<RpcTestInterface> service_;
  static std::unique_ptr<RpcServer> rpc_server_;
  static std::unique_ptr<IoContextPool> pool_;
};

std::unique_ptr<RpcServer> RpcServerTest::rpc_server_;
std::unique_ptr<IoContextPool> RpcServerTest::pool_;

TEST_F(RpcServerTest, Basic) {

}

}  // namespace util
