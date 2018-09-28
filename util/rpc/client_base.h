// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include "util/asio/client_channel.h"
#include "util/rpc/buffered_read_adaptor.h"
#include "util/rpc/frame_format.h"
#include "util/rpc/rpc_envelope.h"
#include "absl/container/flat_hash_map.h"

// Single-threaded, multi-fiber safe rpc client.
namespace util {
namespace rpc {

class ClientBase {
 public:
  using error_code = ClientChannel::error_code;
  using future_code_t = boost::fibers::future<error_code>;

  ClientBase(ClientChannel&& channel) : channel_(std::move(channel)), br_(channel_.socket(), 2048) {
  }

  ClientBase(IoContext& cntx, const std::string& hostname, const std::string& service)
      : ClientBase(ClientChannel(cntx, hostname, service)) {
  }

  ~ClientBase();

  // Blocks at least for 'ms' milliseconds to connect to the host.
  // Should be called once during the initialization phase before sending the requests.
  error_code Connect(uint32_t ms);

  // Write path is "fiber-synchronous", i.e. done inside calling fiber.
  // Which means we should not run this function from io_context loop.
  // Calling from a separate fiber is fine.
  future_code_t Send(Envelope* envelope);

  // Fully fiber-blocking call. Sends and waits until the response is back.
  // The response pair is written into the same buffers.
  error_code SendSync(Envelope* envelope) {
    return Send(envelope).get();
  }

  // Blocks the calling fiber until all the background processes finish.
  void Shutdown();

 private:
  void ReadFiber();
  void FlushFiber();

  void CancelPendingCalls(error_code ec);
  error_code ReadEnvelope();
  error_code PresendChecks();
  error_code FlushSends();
  error_code FlushSendsGuarded();

  error_code CancelSentBufferGuarded(error_code ec);

  RpcId rpc_id_ = 1;
  ClientChannel channel_;
  BufferedReadAdaptor<ClientChannel::socket_t> br_;

  struct PendingCall {
    boost::fibers::promise<error_code> promise;
    Envelope* envelope;

    PendingCall(boost::fibers::promise<error_code> p, Envelope* env)
        : promise(std::move(p)), envelope(env) {
    }
  };

  struct SendItem {
    RpcId rpc_id;
    Envelope* envelope;
    uint8_t frame_buf[rpc::Frame::kMaxByteSize];

    SendItem(RpcId i, Envelope* e) : rpc_id(i), envelope(e) {}
  };

  typedef absl::flat_hash_map<RpcId, PendingCall> PendingMap;
  PendingMap pending_calls_;
  std::vector<SendItem> outgoing_buf_;

  boost::fibers::fiber read_fiber_, flush_fiber_;
  boost::fibers::mutex send_mu_;
  std::vector<boost::asio::const_buffer> write_seq_;
};

}  // namespace rpc
}  // namespace util
