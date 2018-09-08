// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "base/gtest.h"
#include "util/rpc/rpc_connection.h"
#include "util/asio/io_context_pool.h"

namespace util {
class ClientChannel;

namespace rpc {

class TestBridge final : public ConnectionBridge {
 public:
  // header and letter are input/output parameters.
  // HandleEnvelope reads first the input and if everything is parsed fine, it sends
  // back another header, letter pair.
  Status HandleEnvelope(uint64_t rpc_id, base::PODArray<uint8_t>* header,
                        base::PODArray<uint8_t>* letter) override;
};

class TestInterface final : public ServiceInterface {
 public:
  ConnectionBridge* CreateConnectionBridge() override { return new TestBridge{}; }
};

class ServerTest : public testing::Test {
 public:
  ServerTest();

 protected:
  static void SetUpTestCase() {}

  static void TearDownTestCase() {}

  void SetUp() override;

  void TearDown() override;

  std::unique_ptr<TestInterface> service_;
  std::unique_ptr<AcceptServer> server_;
  std::unique_ptr<IoContextPool> pool_;
  std::unique_ptr<ClientChannel> channel_;
  ::boost::system::error_code ec_;
};

}  // namespace rpc
}  // namespace util
