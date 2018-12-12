// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include <chrono>
#include <memory>

#include "absl/strings/numbers.h"
#include "absl/strings/strip.h"
#include "base/logging.h"
#include "base/walltime.h"

#include "util/asio/asio_utils.h"
#include "util/rpc/channel.h"
#include "util/rpc/frame_format.h"
#include "util/rpc/rpc_test_utils.h"

namespace util {
namespace rpc {

using namespace std;
using namespace boost;
using testing::internal::CaptureStderr;
using testing::internal::GetCapturedStderr;


TEST_F(ServerTest, SendOk) {
  Channel client(std::move(*channel_));
  client.Connect(10);

  Envelope envelope;
  envelope.header.resize_fill(14, 1);
  envelope.letter.resize_fill(42, 2);
  Channel::future_code_t fc = client.Send(20, &envelope);
  EXPECT_FALSE(fc.get());
}

TEST_F(ServerTest, ServerStopped) {
  std::unique_ptr<Channel> client(new Channel(std::move(*channel_)));
  client->Connect(10);

  Envelope envelope;
  envelope.header.resize_fill(14, 1);
  envelope.letter.resize_fill(42, 2);

  Channel::future_code_t fc = client->Send(20, &envelope);
  EXPECT_FALSE(fc.get());
  CaptureStderr();
  server_->Stop();
  server_->Wait();

  fc = client->Send(20, &envelope);
  EXPECT_TRUE(fc.get());
  client.reset();
  GetCapturedStderr();
}

TEST_F(ServerTest, Stream) {
  std::unique_ptr<Channel> client(new Channel(std::move(*channel_)));
  system::error_code ec = client->Connect(20);

  ASSERT_FALSE(ec);

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
  ec = client->SendAndReadStream(&envelope, cb);
  EXPECT_FALSE(ec);
  EXPECT_EQ(3, times);
}

TEST_F(ServerTest, Sleep) {
  std::unique_ptr<Channel> client(new Channel(std::move(*channel_)));
  system::error_code ec = client->Connect(20);
  ASSERT_FALSE(ec);

  string header("sleep20");

  Envelope envelope;
  Copy(header, &envelope.header);
  envelope.letter.resize_fill(42, 2);

  ec = client->SendSync(1, &envelope);
  ASSERT_EQ(asio::error::timed_out, ec) << ec.message();   // expect timeout.

  envelope.header.clear();
  ec = client->SendSync(20, &envelope);
  ASSERT_FALSE(ec) << ec.message();   // expect normal execution.
}

}  // namespace rpc
}  // namespace util
