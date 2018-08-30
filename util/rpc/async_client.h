// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "util/asio/channel.h"

// Single-threaded, fiber safe Asynchronous client for rpc communication.
namespace util {
namespace rpc {

class AsyncClient {
 public:
  using error_code = ClientChannel::error_code;

  AsyncClient(ClientChannel&& channel) : channel_(std::move(channel)) {
    SetupReadFiber();
  }

  // Write path is "fiber-synchronous", i.e. done in calling fiber.
  // Which means we should not run this function from io_context loop. From dedicated fiber is fine.
  ::boost::fibers::future<error_code> SendEnvelope(std::string* header, std::string* letter);

 private:
  void SetupReadFiber();

  uint64_t rpc_id_ = 1;

  ClientChannel channel_;
};

}  // namespace rpc
}  // namespace util
