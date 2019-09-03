// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include <chrono>
#include <memory>

#include <boost/asio/write.hpp>

#include "absl/strings/str_cat.h"
#include "absl/strings/strip.h"

#include "base/gtest.h"
#include "base/logging.h"
#include "base/walltime.h"

#include "util/asio/accept_server.h"
#include "util/asio/asio_utils.h"
#include "util/asio/yield.h"

#include "util/rpc/channel.h"
#include "util/rpc/frame_format.h"
#include "util/rpc/rpc_test_utils.h"

namespace util {
namespace rpc {

using namespace std;
using namespace boost;
using asio::ip::tcp;

using testing::internal::CaptureStderr;
using testing::internal::GetCapturedStderr;

class RpcTest : public ServerTest {
 protected:
  asio::mutable_buffer FrameBuffer() {
    size_t fr_sz = frame_.Write(buf_);
    return asio::mutable_buffer(buf_, fr_sz);
  }

  void SetUp() final {
    ServerTest::SetUp();

    channel_.reset(new Channel("localhost", std::to_string(port_), &pool_->GetNextContext()));
    auto ec = channel_->Connect(1000);
    CHECK(!ec) << ec.message();
  }

  void TearDown() final {
    channel_.reset();
    ServerTest::TearDown();
  }

  uint8_t buf_[Frame::kMaxByteSize];
  Frame frame_;
  std::unique_ptr<Channel> channel_;
};

TEST_F(RpcTest, BadHeader) {
  // Must be large enough to pass the initial RPC server read.
  string control("Hello "), message("world!!!");
  asio::write(*sock2_, make_buffer_seq(control, message), ec_);
  ASSERT_FALSE(ec_);
  VLOG(1) << "After the write";
  asio::read(*sock2_, make_buffer_seq(control, message), ec_);
  VLOG(1) << "After the read";

  ASSERT_EQ(asio::error::make_error_code(asio::error::eof), ec_) << ec_.message();
}

TEST_F(RpcTest, HelloFrame) {
  string header("Hello "), letter("world!!!");
  frame_.header_size = header.size();
  frame_.letter_size = letter.size();

  IoContext& io_cntx = sock2_->context();
  io_cntx.AwaitSafe([&] {
    // Write correct frame and the envelope.
    asio::write(*sock2_, make_buffer_seq(FrameBuffer(), header, letter), ec_);
    ASSERT_FALSE(ec_) << ec_.message();

    // Read back envelope.
    ec_ = frame_.Read(sock2_.get());
    ASSERT_FALSE(ec_);
    EXPECT_EQ(frame_.header_size, header.size());
    EXPECT_EQ(frame_.letter_size, letter.size());

    // Read header/letter.
    asio::read(*sock2_, make_buffer_seq(header, letter), ec_);
    ASSERT_FALSE(ec_) << ec_.message();
  });
}

TEST_F(RpcTest, InvalidAndConnect) {
  std::string send_msg(500, 'a');
  IoContext& io_cntx = sock2_->context();
  io_cntx.AwaitSafe([&] {
    asio::write(*sock2_, make_buffer_seq(send_msg), ec_);
    ASSERT_FALSE(ec_);

    ec_ = frame_.Read(sock2_.get());
    ASSERT_TRUE(ec_) << ec_.message();
  });

  frame_.header_size = send_msg.size();
  frame_.letter_size = 0;

  LOG(INFO) << "Before Connect " << sock2_->native_handle();
  ec_ = sock2_->ClientWaitToConnect(1000);
  ASSERT_FALSE(ec_);
  LOG(INFO) << "After Connect " << sock2_->native_handle();

  io_cntx.AwaitSafe([&] {
    asio::write(*sock2_, make_buffer_seq(FrameBuffer(), send_msg), ec_);
    ASSERT_FALSE(ec_) << ec_.message();
    ec_ = frame_.Read(sock2_.get());
    ASSERT_FALSE(ec_) << ec_.message();
  });
}

TEST_F(RpcTest, Repeat) {
  const char kPayload[] = "World!!!";
  string header("repeat3"), message(kPayload);
  frame_.header_size = header.size();
  frame_.letter_size = message.size();

  IoContext& io_cntx = sock2_->context();
  io_cntx.AwaitSafe([&] {
    asio::write(*sock2_, make_buffer_seq(FrameBuffer(), header, message), ec_);
    ASSERT_FALSE(ec_);

    for (unsigned i = 0; i < 3; ++i) {
      ec_ = frame_.Read(sock2_.get());
      EXPECT_FALSE(ec_);

      header.resize(frame_.header_size);
      ASSERT_EQ(frame_.letter_size, message.size());

      asio::read(*sock2_, make_buffer_seq(header, message), ec_);
      ASSERT_FALSE(ec_);
      string expected = absl::StrCat("cont:", i + 1 < 3);
      EXPECT_EQ(expected, header);
      EXPECT_EQ(kPayload, message);
    }
  });
}

TEST_F(RpcTest, SendOk) {
  Envelope envelope;
  envelope.header.resize_fill(14, 1);
  envelope.letter.resize_fill(42, 2);
  Channel::future_code_t fc = channel_->Send(20, &envelope);
  EXPECT_FALSE(fc.get());
}

TEST_F(RpcTest, ServerStopped) {
  Envelope envelope;
  envelope.header.resize_fill(14, 1);
  envelope.letter.resize_fill(42, 2);

  Channel::future_code_t fc = channel_->Send(20, &envelope);
  EXPECT_FALSE(fc.get());
  CaptureStderr();
  server_->Stop();
  server_->Wait();

  fc = channel_->Send(20, &envelope);
  EXPECT_TRUE(fc.get());
  GetCapturedStderr();
  LOG(INFO) << "After channel::Send";
}

TEST_F(RpcTest, Stream) {
  string header("repeat3");

  Envelope envelope;
  Copy(header, &envelope.header);

  envelope.letter.resize_fill(42, 2);

  int times = 0;
  auto cb = [&](Envelope& env) -> system::error_code {
    ++times;
    absl::string_view header(strings::charptr(env.header.data()), env.header.size());
    LOG(INFO) << "Stream resp: " << header;
    if (absl::ConsumePrefix(&header, "cont:")) {
      uint32_t cont = 0;
      CHECK(absl::SimpleAtoi(header, &cont));
      return cont ? system::error_code{} : asio::error::eof;
    }
    return system::errc::make_error_code(system::errc::invalid_argument);
  };
  system::error_code ec = channel_->SendAndReadStream(&envelope, cb);
  EXPECT_FALSE(ec);
  EXPECT_EQ(3, times);
}

TEST_F(RpcTest, Sleep) {
  string header("sleep20");

  Envelope envelope;
  Copy(header, &envelope.header);
  envelope.letter.resize_fill(42, 2);

  system::error_code ec = channel_->SendSync(1, &envelope);
  ASSERT_EQ(asio::error::timed_out, ec) << ec.message();  // expect timeout.

  envelope.header.clear();
  ec = channel_->SendSync(80, &envelope);
  ASSERT_FALSE(ec) << ec.message();  // expect normal execution.
}

static void BM_ChannelConnection(benchmark::State& state) {
  IoContextPool pool(1);
  pool.Run();
  AcceptServer server(&pool);

  TestInterface ti;
  uint16_t port = server.AddListener(0, &ti);

  server.Run();

  FiberSyncSocket socket("localhost", std::to_string(port), &pool.GetNextContext());
  CHECK(!socket.ClientWaitToConnect(500));
  Frame frame;
  std::string send_msg(100, 'a');
  uint8_t buf[Frame::kMaxByteSize];
  BufferType header, letter;
  frame.letter_size = send_msg.size();
  letter.resize(frame.letter_size);

  uint64_t fs = frame.Write(buf);
  BufferType tmp;
  tmp.resize(10000);

  auto buf_seq = make_buffer_seq(asio::buffer(buf, fs), send_msg);
  size_t total_sz = 0, buf_seq_sz = asio::buffer_size(buf_seq);

  socket.context().AwaitSafe([&] {
    system::error_code ec;
    while (state.KeepRunning()) {
      asio::write(socket, buf_seq, ec);
      CHECK(!ec);
      total_sz += buf_seq_sz;

      DCHECK(!ec);
      DCHECK_EQ(0, frame.header_size);
      DCHECK_EQ(letter.size(), frame.letter_size);
      socket.read_some(asio::buffer(tmp), ec);
    }
  });

  state.SetBytesProcessed(total_sz);
  server.Stop();
}
BENCHMARK(BM_ChannelConnection);

}  // namespace rpc
}  // namespace util
