// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <deque>
#include "base/pod_array.h"
#include "util/asio/channel.h"

// Single-threaded, fiber safe Asynchronous client for rpc communication.
namespace util {
namespace rpc {

class AsyncClient {
 public:
  using error_code = ClientChannel::error_code;
  using future_code_t = ::boost::fibers::future<error_code>;
  AsyncClient(ClientChannel&& channel) : channel_(std::move(channel)) {
    SetupReadFiber();
  }

  // Write path is "fiber-synchronous", i.e. done in calling fiber.
  // Which means we should not run this function from io_context loop. From dedicated fiber is fine.
  future_code_t SendEnvelope(base::PODArray<uint8_t>* header, base::PODArray<uint8_t>* letter);

 private:
  void SetupReadFiber();

  uint64_t rpc_id_ = 1;

  ClientChannel channel_;
  struct PendingCall {
    uint64_t rpc_id;
    ::boost::fibers::promise<error_code> p;
  };

  std::deque<PendingCall> calls_;
};

}  // namespace rpc
}  // namespace util
