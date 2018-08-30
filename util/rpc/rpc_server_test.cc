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

#include "util/asio/asio_utils.h"
#include "util/asio/channel.h"
#include "util/rpc/frame_format.h"
#include "util/rpc/rpc_test_utils.h"

namespace util {
namespace rpc {

using namespace std;
using namespace boost;
using asio::ip::tcp;




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
  std::string send_msg(500, 'a');
  ec_ = channel_->Write(make_buffer_seq(send_msg));
  ASSERT_FALSE(ec_);

  Frame frame;

  ec_ = channel_->Read([&frame] (tcp::socket& s) {
    return frame.Read(&s);
  });
  ASSERT_TRUE(ec_) << ec_.message();

  uint8_t buf[Frame::kMaxByteSize];
  frame.header_size = send_msg.size();
  uint64_t fs = frame.Write(buf);
  while (ec_) {
    SleepForMilliseconds(10);
    ec_ = channel_->Write(make_buffer_seq(asio::buffer(buf, fs), send_msg));
  }
  ec_ = channel_->Read([&frame] (tcp::socket& s) {
    return frame.Read(&s);
  });
  ASSERT_FALSE(ec_) << ec_.message();
}



}  // namespace rpc
}  // namespace util
