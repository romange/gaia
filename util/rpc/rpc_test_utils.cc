// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "util/rpc/rpc_test_utils.h"
#include "base/logging.h"

#include "util/asio/channel.h"

namespace util {
namespace rpc {

Status TestBridge::HandleEnvelope(uint64_t rpc_id, base::PODArray<uint8_t>* header,
                        base::PODArray<uint8_t>* letter) {
  LOG(INFO) << "Got " << rpc_id << ", hs=" << header->size() << ", ls=" << letter->size();
  return Status::OK;
}

ServerTest::ServerTest() {}

void ServerTest::SetUp() {
  pool_.reset(new IoContextPool);
  pool_->Run();
  service_.reset(new TestInterface);
  server_.reset(new AcceptServer(pool_.get()));
  uint16_t port = service_->Listen(0, server_.get());

  server_->Run();
  channel_.reset(new ClientChannel(pool_->GetNextContext(), "127.0.0.1",
                 std::to_string(port)));
  ec_ = channel_->Connect(100);
  CHECK(!ec_) << ec_.message();
}

void ServerTest::TearDown() {
  server_.reset();
  channel_.reset();
  pool_->Stop();
}

}  // namespace rpc
}  // namespace util

