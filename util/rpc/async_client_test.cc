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
#include "util/rpc/client_base.h"
#include "util/rpc/frame_format.h"
#include "util/rpc/rpc_test_utils.h"

namespace util {
namespace rpc {

using namespace std;
using namespace boost;
using testing::internal::CaptureStderr;
using testing::internal::GetCapturedStderr;


TEST_F(ServerTest, SendOk) {
  ClientBase client(std::move(*channel_));
  client.Connect(10);

  Envelope envelope;
  envelope.header.resize_fill(14, 1);
  envelope.letter.resize_fill(42, 2);
  ClientBase::future_code_t fc = client.Send(&envelope);
  EXPECT_FALSE(fc.get());
}

TEST_F(ServerTest, ServerStopped) {
  std::unique_ptr<ClientBase> client(new ClientBase(std::move(*channel_)));
  client->Connect(10);

  Envelope envelope;
  envelope.header.resize_fill(14, 1);
  envelope.letter.resize_fill(42, 2);

  ClientBase::future_code_t fc = client->Send(&envelope);
  EXPECT_FALSE(fc.get());
  CaptureStderr();
  server_->Stop();
  server_->Wait();

  fc = client->Send(&envelope);
  EXPECT_TRUE(fc.get());
  client.reset();
  GetCapturedStderr();
}

TEST_F(ServerTest, Stream) {
  std::unique_ptr<ClientBase> client(new ClientBase(std::move(*channel_)));
  client->Connect(20);

  string header("repeat3");

  Envelope envelope;
  Copy(header, &envelope.header);

  envelope.letter.resize_fill(42, 2);

  int times = 0;
  auto cb = [&](Envelope& env) {
    ++times;
    absl::string_view header(strings::charptr(env.header.data()), env.header.size());
    LOG(INFO) << "Stream resp: " << header;
    if (absl::ConsumePrefix(&header, "cont:")) {
      uint32_t cont = 0;
      CHECK(absl::SimpleAtoi(header, &cont));
      return cont > 0;
    }
    return false;
  };
  ClientBase::error_code ec = client->SendAndReadStream(&envelope, cb);
  EXPECT_FALSE(ec);
}

}  // namespace rpc
}  // namespace util
