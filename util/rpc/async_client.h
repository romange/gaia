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
    read_fiber_ = ::boost::fibers::fiber(&AsyncClient::ReadFiber, this);
  }

  ~AsyncClient();

  // Write path is "fiber-synchronous", i.e. done inside calling fiber.
  // Which means we should not run this function from io_context loop. From dedicated fiber is fine.
  future_code_t SendEnvelope(base::PODArray<uint8_t>* header, base::PODArray<uint8_t>* letter);

  void Shutdown();

 private:
  void ReadFiber();

  void FlushPendingCalls(error_code ec);
  error_code ReadEnvelope(ClientChannel::socket_t* sock);

  uint64_t rpc_id_ = 1;
  ClientChannel channel_;

  struct PendingCall {
    uint64_t rpc_id;
    ::boost::fibers::promise<error_code> promise;
    base::PODArray<uint8_t> *header, *letter;

    PendingCall(uint64_t r, ::boost::fibers::promise<error_code> p, base::PODArray<uint8_t> *h,
                base::PODArray<uint8_t> *l)
       : rpc_id(r), promise(std::move(p)), header(h), letter(l) {}
  };

  std::deque<PendingCall> calls_;
  ::boost::fibers::fiber read_fiber_;
};

}  // namespace rpc
}  // namespace util
