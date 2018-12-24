// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include <chrono>
#include <memory>

#include "absl/strings/str_cat.h"
#include "base/gtest.h"
#include "base/logging.h"
#include "base/walltime.h"

#if (__GNUC__ > 4)
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif

#include "util/asio/accept_server.h"
#include "util/asio/asio_utils.h"
#include "util/asio/client_channel.h"
#include "util/asio/yield.h"
#include "util/rpc/frame_format.h"
#include "util/rpc/rpc_test_utils.h"

namespace util {
namespace rpc {

using namespace std;
using namespace boost;
using asio::ip::tcp;

class RpcTest : public ServerTest {

 protected:
  asio::mutable_buffer FrameBuffer() {
    size_t fr_sz = frame_.Write(buf_);
    return asio::mutable_buffer(buf_, fr_sz);
  }

  uint8_t buf_[Frame::kMaxByteSize];
  Frame frame_;
};

TEST_F(RpcTest, BadHeader) {
  // Must be large enough to pass the initial RPC server read.
  string control("Hello "), message("world!!!");
  ec_ = socket_->Write(make_buffer_seq(control, message));
  ASSERT_FALSE(ec_);

  ec_ = socket_->Read(make_buffer_seq(control, message));
  ASSERT_EQ(asio::error::make_error_code(asio::error::eof), ec_) << ec_.message();
}

TEST_F(RpcTest, Basic) {
  // Must be large enough to pass the initial RPC server read.
  string header("Hello "), message("world!!!");
  frame_.header_size = header.size();
  frame_.letter_size = message.size();

  ec_ = socket_->Write(make_buffer_seq(FrameBuffer(), header, message));
  ASSERT_FALSE(ec_);

  ec_ = socket_->Apply([this](auto& s) {
    return frame_.Read(&s);
  });
  ASSERT_FALSE(ec_);
  EXPECT_EQ(frame_.header_size, header.size());
  EXPECT_EQ(frame_.letter_size, message.size());

  ec_ = socket_->Read(make_buffer_seq(header, message));
  ASSERT_FALSE(ec_) << ec_.message();
}


TEST_F(RpcTest, Socket) {
  std::string send_msg(500, 'a');
  ec_ = socket_->Write(make_buffer_seq(send_msg));
  ASSERT_FALSE(ec_);

  ec_ = socket_->Apply([this] (tcp::socket& s) {
    return frame_.Read(&s);
  });
  ASSERT_TRUE(ec_) << ec_.message();

  frame_.header_size = send_msg.size();
  frame_.letter_size = 0;

  // Wait for reconnect.
  while (ec_) {
    SleepForMilliseconds(10);
    ec_ = socket_->Write(make_buffer_seq(FrameBuffer(), send_msg));
  }
  ec_ = socket_->Apply([this] (tcp::socket& s) {
    return frame_.Read(&s);
  });
  ASSERT_FALSE(ec_) << ec_.message();
}

TEST_F(RpcTest, SocketRead) {
  tcp::socket& sock = socket_->socket();
  ASSERT_TRUE(sock.non_blocking());
  ec_ = socket_->Write(make_buffer_seq(FrameBuffer()));
  ASSERT_FALSE(ec_);
  // std::vector<uint8_t> tmp(10000);
  //size_t sz = sock.read_some(asio::buffer(tmp));
  // LOG(INFO) << sz;
}

TEST_F(RpcTest, Repeat) {
  tcp::socket& sock = socket_->socket();
  ASSERT_TRUE(sock.non_blocking());

  const char kPayload[] = "World!!!";
  string header("repeat3"), message(kPayload);
  frame_.header_size = header.size();
  frame_.letter_size = message.size();

  ec_ = socket_->Write(make_buffer_seq(FrameBuffer(), header, message));
  ASSERT_FALSE(ec_);


  for (unsigned i = 0; i < 3; ++i) {
    ec_ = socket_->Apply([&] (tcp::socket& s) {
      return frame_.Read(&s);
    });
    EXPECT_FALSE(ec_);

    header.resize(frame_.header_size);
    ASSERT_EQ(frame_.letter_size, message.size());

    ec_ = socket_->Read(make_buffer_seq(header, message));
    ASSERT_FALSE(ec_);
    string expected = absl::StrCat("cont:", i + 1 < 3);
    EXPECT_EQ(expected, header);
    EXPECT_EQ(kPayload, message);
  }
}


static void BM_ChannelConnection(benchmark::State& state) {
  IoContextPool pool(1);
  pool.Run();
  AcceptServer server(&pool);

  TestInterface ti;
  uint16_t port = server.AddListener(0, &ti);

  server.Run();

  ReconnectableSocket recon_socket("127.0.0.1", std::to_string(port), &pool.GetNextContext());
  CHECK(!recon_socket.Connect(500));
  Frame frame;
  std::string send_msg(100, 'a');
  uint8_t buf[Frame::kMaxByteSize];
  BufferType header, letter;
  frame.letter_size = send_msg.size();
  letter.resize(frame.letter_size);

  uint64_t fs = frame.Write(buf);
  auto& socket = recon_socket.socket();
  BufferType tmp;
  tmp.resize(10000);

  auto buf_seq = make_buffer_seq(asio::buffer(buf, fs), send_msg);
  size_t total_sz = 0, buf_seq_sz = asio::buffer_size(buf_seq);

  while (state.KeepRunning()) {
    system::error_code ec = recon_socket.Write(buf_seq);
    CHECK(!ec);
    total_sz += buf_seq_sz;

    DCHECK(!ec);
    DCHECK_EQ(0, frame.header_size);
    DCHECK_EQ(letter.size(), frame.letter_size);
    socket.read_some(asio::buffer(tmp), ec);
  }
  state.SetBytesProcessed(total_sz);
  server.Stop();
}
BENCHMARK(BM_ChannelConnection);

}  // namespace rpc
}  // namespace util
