// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include <chrono>
#include <memory>

#include "base/gtest.h"
#include "base/logging.h"
#include "base/walltime.h"

#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"

#include "util/asio/io_context_pool.h"
#include "util/rpc/rpc_server.h"

namespace util {

using namespace boost;
using asio::ip::tcp;

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
    pool_.reset(new IoContextPool);
  }

  static void TearDownTestCase() {
    pool_.reset();
  }

  void SetUp() override {
    service_.reset(new RpcTestInterface);
    rpc_server_.reset(new RpcServer(0));
    rpc_server_->BindTo(service_.get());
    rpc_server_->Run(pool_.get());
  }

  void TearDown() override {}

  std::unique_ptr<RpcTestInterface> service_;
  std::unique_ptr<RpcServer> rpc_server_;
  static std::unique_ptr<IoContextPool> pool_;
};

std::unique_ptr<IoContextPool> RpcServerTest::pool_;

TEST_F(RpcServerTest, Basic) {
  auto& cntx = pool_->GetNextContext();
  tcp::socket sock(cntx);
  tcp::endpoint endpoint(tcp::v4(), rpc_server_->port());
  system::error_code ec;
  sock.connect(endpoint, ec);

  EXPECT_EQ(system::error_code{}, ec);
}

}  // namespace util
