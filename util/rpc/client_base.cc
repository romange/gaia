// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include <boost/fiber/future.hpp>

#include "util/rpc/client_base.h"

#include "base/logging.h"
#include "util/asio/asio_utils.h"
#include "util/rpc/frame_format.h"
#include "util/rpc/rpc_envelope.h"

namespace util {
namespace rpc {

DEFINE_uint32(rpc_client_pending_limit, 1 << 17,
              "How many outgoing requests we are ready to accommodate before rejecting "
              "a new RPC request");

DEFINE_uint32(rpc_client_queue_size, 128,
              "The size of the outgoing batch queue that contains envelopes waiting to send.");

using namespace boost;
using asio::ip::tcp;
using folly::RWSpinLock;

namespace {

template <typename R>
fibers::future<std::decay_t<R>> make_ready(R&& r) {
  fibers::promise<std::decay_t<R>> p;
  fibers::future<std::decay_t<R>> res = p.get_future();
  p.set_value(std::forward<R>(r));

  return res;
}

}  // namespace

ClientBase::~ClientBase() {
  Shutdown();

  VLOG(1) << "Before ReadFiberJoin";
  CHECK(read_fiber_.joinable());
  read_fiber_.join();
  flush_fiber_.join();
}

void ClientBase::Shutdown() {
  channel_.Shutdown();
}

auto ClientBase::Connect(uint32_t ms) -> error_code {
  CHECK(!read_fiber_.joinable());
  error_code ec = channel_.Connect(ms);

  IoContext& context = channel_.context();
  context.PostSynchronous([this] {
    read_fiber_ = fibers::fiber(&ClientBase::ReadFiber, this);
    flush_fiber_ = fibers::fiber(&ClientBase::FlushFiber, this);
  });

  return ec;
}

auto ClientBase::PresendChecks() -> error_code {
  if (channel_.is_shut_down()) {
    return asio::error::shut_down;
  }

  if (channel_.status()) {
    return channel_.status();
  }

  if (pending_calls_size_.load(std::memory_order_relaxed) >= FLAGS_rpc_client_pending_limit) {
    return asio::error::no_buffer_space;
  }

  error_code ec;

  if (outgoing_buf_size_.load(std::memory_order_relaxed) >= FLAGS_rpc_client_queue_size) {
    ec = FlushSends();
  }
  return ec;
}

auto ClientBase::Send(Envelope* envelope) -> future_code_t {
  DCHECK(read_fiber_.joinable());

  // ----
  fibers::promise<error_code> p;
  fibers::future<error_code> res = p.get_future();
  error_code ec = PresendChecks();

  if (ec) {
    p.set_value(ec);
    return res;
  }

  // We protect against Send thread vs IoContext thread data races.
  // Fibers inside IoContext thread do not have to protect against each other since
  // they do not cause data races. So we use lock_shared as "no-op" lock that only becomes
  // relevant if someone outside IoContext thread locks exclusively.
  // Also this lock allows multi-threaded access for Send operation.
  bool lock_exclusive = OutgoingBufLock();
  RpcId id = rpc_id_++;

  outgoing_buf_.emplace_back(SendItem(id, PendingCall{std::move(p), envelope}));
  outgoing_buf_size_.store(outgoing_buf_.size(), std::memory_order_relaxed);

  OutgoingBufUnlock(lock_exclusive);

  return res;
}

auto ClientBase::SendAndReadStream(Envelope* msg, MessageCallback cb) -> error_code {
  DCHECK(read_fiber_.joinable());

  // ----
  error_code ec = PresendChecks();
  if (ec) {
    return ec;
  }

  fibers::promise<error_code> p;
  fibers::future<error_code> future = p.get_future();

  // We protect against Send thread vs IoContext thread data races.
  // Fibers inside IoContext thread do not have to protect against each other.
  // Therefore use lock_shared as "no-op" lock that only becomes
  // relevant if someone outside IoContext thread locks exclusively.
  // Also this lock allows multi-threaded access for Send operation.
  bool exclusive = OutgoingBufLock();

  RpcId id = rpc_id_++;

  outgoing_buf_.emplace_back(SendItem(id, PendingCall{std::move(p), msg, std::move(cb)}));
  outgoing_buf_size_.store(outgoing_buf_.size(), std::memory_order_relaxed);

  OutgoingBufUnlock(exclusive);
  ec = future.get();

  return ec;
}

void ClientBase::ReadFiber() {
  CHECK(channel_.context().get_executor().running_in_this_thread());

  VLOG(1) << "Start ReadFiber on socket " << channel_.handle();

  error_code ec = channel_.WaitForReadAvailable();
  while (!channel_.is_shut_down()) {
    if (ec) {
      LOG(WARNING) << "Error reading envelope " << ec << " " << ec.message();

      CancelPendingCalls(ec);
      ec.clear();
      continue;
    }

    if (auto ch_st = channel_.status()) {
      ec = channel_.WaitForReadAvailable();
      VLOG(1) << "Channel status " << ch_st << " Read available st: " << ec;
      continue;
    }
    ec = channel_.Apply([this] { return this->ReadEnvelope(); });
  }
  CancelPendingCalls(ec);
  VLOG(1) << "Finish ReadFiber on socket " << channel_.handle();
}

void ClientBase::FlushFiber() {
  using namespace std::chrono_literals;
  CHECK(channel_.context().get_executor().running_in_this_thread());

  while (true) {
    this_fiber::sleep_for(100us);
    if (channel_.is_shut_down())
      break;

    if (outgoing_buf_size_.load(std::memory_order_acquire) == 0 || !send_mu_.try_lock())
      continue;
    VLOG(1) << "FlushFiber::FlushSendsGuarded";
    FlushSendsGuarded();
    outgoing_buf_size_.store(outgoing_buf_.size(), std::memory_order_release);

    send_mu_.unlock();  // releases the fence
  }
}

auto ClientBase::FlushSends() -> error_code {
  // We call FlushSendsGuarded directly from Send fiber because it calls socket.Write
  // synchronously and we can not Post blocking function into io_context.
  std::lock_guard<fibers::mutex> guard(send_mu_);

  error_code ec;

  // We use `while` because multiple fibers might fill outgoing_buf_
  // and when the current fiber resumes, the buffer might be full again.
  while (outgoing_buf_.size() >= FLAGS_rpc_client_queue_size) {
    ec = FlushSendsGuarded();
  }
  outgoing_buf_size_.store(outgoing_buf_.size(), std::memory_order_relaxed);

  return ec;  // Return the last known status code.
}

auto ClientBase::FlushSendsGuarded() -> error_code {
  error_code ec;
  // This function runs only in IOContext thread. Therefore only
  if (outgoing_buf_.empty())
    return ec;

  ec = channel_.status();
  if (ec) {
    CancelSentBufferGuarded(ec);
    return ec;
  }

  // The following section is CPU-only - No IO blocks.
  {
    RWSpinLock::ReadHolder holder(buf_lock_);  // protect outgoing_buf_ against Send path

    size_t count = outgoing_buf_.size();
    write_seq_.resize(count * 3);
    frame_buf_.resize(count);
    for (size_t i = 0; i < count; ++i) {
      auto& p = outgoing_buf_[i];
      Frame f(p.first, p.second.envelope->header.size(), p.second.envelope->letter.size());
      size_t sz = f.Write(frame_buf_[i].data());

      write_seq_[3 * i] = asio::buffer(frame_buf_[i].data(), sz);
      write_seq_[3 * i + 1] = asio::buffer(p.second.envelope->header);
      write_seq_[3 * i + 2] = asio::buffer(p.second.envelope->letter);
    }

    // Fill the pending call before the socket.Write() because otherwise in case it blocks
    // *after* it sends, the current fiber might resume after Read fiber receives results
    // and it would not find them inside pending_calls_.
    pending_calls_size_.fetch_add(count, std::memory_order_relaxed);
    for (size_t i = 0; i < count; ++i) {
      auto& item = outgoing_buf_[i];
      auto emplace_res = pending_calls_.emplace(item.first, std::move(item.second));
      CHECK(emplace_res.second);
    }
    outgoing_buf_.clear();
  }

  // Interrupt point during which outgoing_buf_ could grow.
  // We do not lock because this function is the only one that writes into channel and it's
  // guarded by send_mu_.
  ec = channel_.Write(write_seq_);
  if (ec) {
    // I do not know if we need to flush everything but I prefer doing it to make it simpler.
    CancelPendingCalls(ec);
    return ec;
  }

  return ec;
}

void ClientBase::CancelSentBufferGuarded(error_code ec) {
  std::vector<SendItem> tmp;

  buf_lock_.lock_shared();
  tmp.swap(outgoing_buf_);
  buf_lock_.unlock_shared();

  for (auto& item : tmp) {
    auto promise = std::move(item.second.promise);
    promise.set_value(ec);
  }
}

auto ClientBase::ReadEnvelope() -> error_code {
  Frame f;
  error_code ec = f.Read(&br_);
  if (ec)
    return ec;

  VLOG(2) << "Got rpc_id " << f.rpc_id << " from socket " << channel_.handle();

  auto it = pending_calls_.find(f.rpc_id);
  if (it == pending_calls_.end()) {
    // It might happens if for some reason we flushed pending_calls_ but the envelope somehow
    // reached us. We just consume it.

    VLOG(1) << "Unknown id " << f.rpc_id;
    Envelope envelope(f.header_size, f.letter_size);

    // ReadEnvelope is called via Channel::Apply, so no need to call it here.
    ec = br_.Read(envelope.buf_seq());
    return ec;
  }

  // -- NO interrupt section begin
  PendingCall& call = it->second;
  Envelope* env = call.envelope;
  env->Resize(f.header_size, f.letter_size);
  bool is_stream = static_cast<bool>(call.cb);

  if (is_stream) {
    VLOG(1) << "Processing stream";
    ec = br_.Read(env->buf_seq());
    if (!ec) {
      HandleStreamResponse(f.rpc_id);
    }

    return ec;
  }

  fibers::promise<error_code> promise = std::move(call.promise);
  // We erase before reading from the socket/setting promise because pending_calls_ might change
  // when we resume after IO and 'it' will be invalidated.
  pending_calls_.erase(it);
  pending_calls_size_.fetch_sub(1, std::memory_order_relaxed);
  // -- NO interrupt section end

  ec = br_.Read(env->buf_seq());
  promise.set_value(ec);

  return ec;
}

void ClientBase::HandleStreamResponse(RpcId rpc_id) {
  auto it = pending_calls_.find(rpc_id_);
  if (it == pending_calls_.end()) {
    return;  // Might happen if pending_calls_ was cancelled when we read the envelope.
  }
  PendingCall& call = it->second;
  bool to_continue = call.cb(*call.envelope);
  if (to_continue)
    return;

  // We finished processing the stream.
  // Keep the promise on the stack and erase from pending_calls_ first because
  // set_value might context switch and invalidate 'it'.
  auto promise = std::move(call.promise);
  pending_calls_.erase(it);
  promise.set_value(error_code{});
}

void ClientBase::CancelPendingCalls(error_code ec) {
  PendingMap tmp;
  tmp.swap(pending_calls_);
  pending_calls_size_.store(0, std::memory_order_relaxed);

  // promise might interrupt so we want to swap into local variable to allow stable iteration
  // over the map. In case pending_calls_ did not change we swap back to preserve already allocated
  // map.
  for (auto& c : tmp) {
    c.second.promise.set_value(ec);
  }
  tmp.clear();
  if (pending_calls_.empty()) {
    tmp.swap(pending_calls_);
  }
}

}  // namespace rpc
}  // namespace util
