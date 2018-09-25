// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include <chrono>
#include <memory>

#include "base/logging.h"

#include "util/asio/asio_utils.h"
#include "util/rpc/client_base.h"
#include "util/rpc/frame_format.h"
#include "util/rpc/rpc_test_utils.h"

namespace util {
namespace rpc {

using namespace std;
using namespace boost;


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

  server_->Stop();
  server_->Wait();
  fc = client->Send(&envelope);
  EXPECT_TRUE(fc.get());
  client.reset();
}

}  // namespace rpc
}  // namespace util
