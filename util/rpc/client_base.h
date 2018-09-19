// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <deque>
#include "base/pod_array.h"
#include "util/asio/client_channel.h"

// Single-threaded, fiber safe rpc client.
namespace util {
namespace rpc {

class ClientBase {
 public:
  using error_code = ClientChannel::error_code;
  using future_code_t = ::boost::fibers::future<error_code>;

  ClientBase(ClientChannel&& channel) : channel_(std::move(channel)) {
    read_fiber_ = ::boost::fibers::fiber(&ClientBase::ReadFiber, this);
  }

  ClientBase(::boost::asio::io_context& cntx, const std::string& hostname,
             const std::string& service) {
    ClientChannel channel(cntx, hostname, service);

    ClientBase(std::move(channel));
  }

  ~ClientBase();

  // Blocks at least for 'ms' milliseconds to connect to the host.
  // Should be called once during the initialization phase before sending the requests.
  error_code Connect(uint32_t ms) {
    return channel_.Connect(ms);
  }

  // Write path is "fiber-synchronous", i.e. done inside calling fiber.
  // Which means we should not run this function from io_context loop.
  // Calling from a separate fiber is fine.
  future_code_t SendEnvelope(base::PODArray<uint8_t>* header, base::PODArray<uint8_t>* letter);

  // Fully fiber-blocking call. Sends and waits until the response is back.
  // The response pair is written into the same buffers.
  error_code SendEnvelopeSync(base::PODArray<uint8_t>* header, base::PODArray<uint8_t>* letter) {
    return SendEnvelope(header, letter).get();
  }

  // Blocks the calling fiber until all the background processes finish.
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
