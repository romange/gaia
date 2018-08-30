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
  rpc_server_.reset(new Server(0));
  rpc_server_->BindTo(service_.get());
  rpc_server_->Run(pool_.get());
  channel_.reset(new ClientChannel(pool_->GetNextContext(), "127.0.0.1",
                 std::to_string(rpc_server_->port())));
  ec_ = channel_->Connect(100);
  CHECK(!ec_) << ec_.message();
}

void ServerTest::TearDown() {
  rpc_server_.reset();
  channel_.reset();
  pool_->Stop();
  pool_->Join();
  pool_.reset();
}

}  // namespace rpc
}  // namespace util

