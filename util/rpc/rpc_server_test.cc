// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include <chrono>
#include <memory>

#include "base/gtest.h"
#include "base/logging.h"
#include "base/walltime.h"

#if (__GNUC__ > 4)
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif

#include "util/asio/io_context_pool.h"
#include "util/asio/asio_utils.h"
#include "util/asio/channel.h"
#include "util/rpc/rpc_server.h"
#include "util/rpc/frame_format.h"

namespace util {
namespace rpc {

using namespace std;
using namespace boost;
using asio::ip::tcp;

class TestBridge final : public ConnectionBridge {
 public:
  // header and letter are input/output parameters.
  // HandleEnvelope reads first the input and if everything is parsed fine, it sends
  // back another header, letter pair.
  Status HandleEnvelope(uint64_t rpc_id, base::PODArray<uint8_t>* header,
                        base::PODArray<uint8_t>* letter) override {
    return Status::OK;
  }
};

class TestInterface final : public ServiceInterface {
 public:
  ConnectionBridge* CreateConnectionBridge() override { return new TestBridge{}; }
};

class ServerTest : public testing::Test {
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
    service_.reset(new TestInterface);
    rpc_server_.reset(new Server(0));
    rpc_server_->BindTo(service_.get());
    rpc_server_->Run(pool_.get());
    channel_.reset(new Channel(pool_->GetNextContext()));
    tcp::endpoint endpoint(tcp::v4(), rpc_server_->port());

    ec_ = channel_->Connect(endpoint, 100);
    CHECK(!ec_) << ec_.message();
  }

  void TearDown() override {
    rpc_server_.reset();
  }

  std::unique_ptr<TestInterface> service_;
  std::unique_ptr<Server> rpc_server_;
  static std::unique_ptr<IoContextPool> pool_;
  std::unique_ptr<Channel> channel_;
  system::error_code ec_;
};

std::unique_ptr<IoContextPool> ServerTest::pool_;


TEST_F(ServerTest, BadHeader) {
  // Must be large enough to pass the initial RPC server read.
  string control("Hello "), message("world!!!");
  ec_ = channel_->Write(make_buffer_seq(control, message));
  ASSERT_FALSE(ec_);

  asio::read(channel_->socket(), make_buffer_seq(control, message), ec_);
  ASSERT_EQ(asio::error::make_error_code(asio::error::eof), ec_) << ec_.message();
}

TEST_F(ServerTest, Basic) {
  // Must be large enough to pass the initial RPC server read.
  string header("Hello "), message("world!!!");
  uint8_t buf[Frame::kMaxByteSize];

  Frame frame(1, header.size(), message.size());
  size_t fr_sz = frame.Write(buf);

  ec_ = channel_->Write(make_buffer_seq(asio::buffer(buf, fr_sz), header, message));
  ASSERT_FALSE(ec_);

  ec_ = frame.Read(&channel_->socket());
  ASSERT_FALSE(ec_);
  EXPECT_EQ(frame.header_size, header.size());
  EXPECT_EQ(frame.letter_size, message.size());

  asio::read(channel_->socket(), make_buffer_seq(header, message), ec_);
  ASSERT_FALSE(ec_) << ec_.message();
}


TEST_F(ServerTest, Socket) {
  tcp::resolver resolver{pool_->GetNextContext()};
  auto result = resolver.resolve(tcp::v4(), "localhost", std::to_string(rpc_server_->port()));
  ASSERT_EQ(1, result.size());

  ASSERT_TRUE(result.begin() != result.end());
  const auto& ep = *result.begin();
  Channel channel(pool_->GetNextContext());

  system::error_code ec = channel.Connect(ep, 100);
  ASSERT_FALSE(ec);
  std::string send_msg(500, 'a');
  ec_ = channel.Write(make_buffer_seq(send_msg));

  ASSERT_FALSE(ec_);
}



}  // namespace rpc
}  // namespace util
