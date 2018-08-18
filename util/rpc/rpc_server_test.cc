// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include <chrono>
#include <memory>
#include <experimental/optional>

#include "base/gtest.h"
#include "base/logging.h"
#include "base/walltime.h"

#if (__GNUC__ > 4)
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif

#include "util/asio/io_context_pool.h"
#include "util/asio/asio_utils.h"
#include "util/rpc/rpc_server.h"

namespace util {

using namespace std;
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
    pool_->Run();
  }

  static void TearDownTestCase() {
    pool_->Stop();
    pool_->Join();
    pool_.reset();
  }

  void SetUp() override {
    service_.reset(new RpcTestInterface);
    rpc_server_.reset(new RpcServer(0));
    rpc_server_->BindTo(service_.get());
    rpc_server_->Run(pool_.get());
    sock_.reset(new tcp::socket(pool_->GetNextContext()));

    tcp::endpoint endpoint(tcp::v4(), rpc_server_->port());

    sock_->connect(endpoint, ec_);
    CHECK(!ec_) << ec_.message();
  }

  void TearDown() override {
    rpc_server_.reset();
  }

  std::unique_ptr<RpcTestInterface> service_;
  std::unique_ptr<RpcServer> rpc_server_;
  static std::unique_ptr<IoContextPool> pool_;
  std::unique_ptr<tcp::socket> sock_;
  system::error_code ec_;
};

std::unique_ptr<IoContextPool> RpcServerTest::pool_;


TEST_F(RpcServerTest, BadHeader) {
  // Must be large enough to pass the initial RPC server read.
  string control("Hello "), message("world!!!");

  size_t sz = asio::write(*sock_, make_buffer_seq(control, message), ec_);
  ASSERT_FALSE(ec_);
  EXPECT_EQ(control.size() + message.size(), sz);

  sz = asio::read(*sock_, make_buffer_seq(control, message), ec_);
  ASSERT_EQ(asio::error::make_error_code(asio::error::eof), ec_) << ec_.message();
}

}  // namespace util
