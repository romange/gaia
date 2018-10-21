// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include "absl/container/flat_hash_map.h"

#include "base/RWSpinLock.h"  //
#include "base/wheel_timer.h"

#include "util/asio/client_channel.h"
#include "util/asio/periodic_task.h"

#include "util/rpc/buffered_read_adaptor.h"
#include "util/rpc/frame_format.h"
#include "util/rpc/rpc_envelope.h"

namespace util {
namespace rpc {

// Fiber-safe rpc client.
// Send(..) is also thread-safe and may be used from multiple threads differrent than
// of IoContext containing the channel, however it might incur performance penalty.
// Therefore to achieve maximal performance - it's advised to ClientBase only from IoContext thread.
class ClientBase {
 public:
  using error_code = ClientChannel::error_code;
  using future_code_t = boost::fibers::future<error_code>;

  // Returns boost::asio::error::eof if the Stream has been finished,
  // if bool(error_code) returns true, aborts receiving the stream and returns the error.
  using MessageCallback = std::function<error_code(Envelope&)>;

  ClientBase(ClientChannel&& channel) : channel_(std::move(channel)), br_(channel_.socket(), 2048) {
  }

  ClientBase(IoContext& cntx, const std::string& hostname, const std::string& service)
      : ClientBase(ClientChannel(cntx, hostname, service)) {
  }

  ~ClientBase();

  // Blocks at least for 'ms' milliseconds to connect to the host.
  // Should be called once during the initialization phase before sending the requests.
  error_code Connect(uint32_t ms);

  // Thread-safe function.
  // Sends the envelope and returns the future to the response status code.
  // Future is realized when response is received and serialized into the same envelope.
  // Send() might block therefore it should not be called directly from IoContext loop (post).
  future_code_t Send(uint32_t deadline_msec, Envelope* envelope);

  // Fiber-blocking call. Sends and waits until the response is back.
  // Similarly to Send, the response is written into the same envelope.
  error_code SendSync(uint32_t deadline_msec, Envelope* envelope) {
    return Send(deadline_msec, envelope).get();
  }

  // Sends a msg and wait to receive a stream of envelopes.
  // For each envelope MessageCallback is called.
  // MessageCallback should return True if more items are expected in the stream.
  // i.e. Envelope should contain stream-related information to allow MessageCallback to
  // decide whether more envelopes should come.
  error_code SendAndReadStream(Envelope* msg, MessageCallback cb);

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

  void CancelSentBufferGuarded(error_code ec);
  void ExpirePending(RpcId id);

  bool OutgoingBufLock() {
    bool lock_exclusive = !channel_.context().InContextThread();

    if (lock_exclusive)
      buf_lock_.lock();
    else
      buf_lock_.lock_shared();

    return lock_exclusive;
  }

  void OutgoingBufUnlock(bool exclusive) {
    if (exclusive)
      buf_lock_.unlock();
    else
      buf_lock_.unlock_shared();
  }

  void HandleStreamResponse(RpcId rpc_id);


  RpcId next_send_rpc_id_ = 1;
  ClientChannel channel_;
  BufferedReadAdaptor<ClientChannel::socket_t> br_;
  typedef boost::fibers::promise<error_code> EcPromise;

  struct PendingCall {
    EcPromise promise;
    Envelope* envelope;

    MessageCallback cb;  // for Stream response.

    PendingCall(EcPromise p, Envelope* env, MessageCallback mcb = MessageCallback{})
      : promise(std::move(p)), envelope(env), cb(std::move(mcb)) {
    }

    std::unique_ptr<base::TimerEventInterface> expiry;
  };

  // The flow is as follows:
  // Send fiber enques requests into outgoing_buf_. It's thread-safe, protected by spinlock.
  // FlushSendsGuarded flushes outgoing queue into the socket. ReadFiber receives envelopes and
  // triggers the receive flow: Stream Handler and finally realizes future signalling the end of
  // rpc call.
  typedef std::pair<RpcId, PendingCall> SendItem;

  typedef absl::flat_hash_map<RpcId, PendingCall> PendingMap;
  PendingMap pending_calls_;
  std::atomic_ulong pending_calls_size_{0};

  folly::RWSpinLock buf_lock_;
  std::vector<SendItem> outgoing_buf_;  // protected by buf_lock_.
  std::atomic_ulong outgoing_buf_size_{0};

  boost::fibers::fiber read_fiber_, flush_fiber_;
  boost::fibers::mutex send_mu_;
  std::vector<boost::asio::const_buffer> write_seq_;
  base::PODArray<std::array<uint8_t, rpc::Frame::kMaxByteSize>> frame_buf_;
  base::TimerWheel expire_timer_;
  std::unique_ptr<PeriodicTask> expiry_task_;
};

}  // namespace rpc
}  // namespace util
