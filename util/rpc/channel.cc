// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include <boost/asio/write.hpp>
#include <boost/fiber/future.hpp>
#include <chrono>

#include "util/rpc/channel.h"

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
using namespace std;
using asio::ip::tcp;
using folly::RWSpinLock;
namespace error = asio::error;

namespace {

bool IsExpectedFinish(system::error_code ec) {
  return ec == error::eof || ec == error::operation_aborted || ec == error::connection_reset ||
         ec == error::not_connected;
}

constexpr uint32_t kTickPrecision = 3;  // 3ms per timer tick.

}  // namespace

Channel::~Channel() {
  Shutdown();

  CHECK(read_fiber_.joinable());
  read_fiber_.join();
  flush_fiber_.join();
  VLOG(1) << "After ReadFiberJoin";
}

void Channel::Shutdown() {
  error_code ec;
  socket_->Shutdown(ec);
}

auto Channel::Connect(uint32_t ms) -> error_code {
  CHECK(!read_fiber_.joinable());
  error_code ec = socket_->ClientWaitToConnect(ms);

  IoContext& context = socket_->context();
  context.Await([this] {
    read_fiber_ = fibers::fiber(&Channel::ReadFiber, this);
    flush_fiber_ = fibers::fiber(&Channel::FlushFiber, this);
  });
  expiry_task_.reset(new PeriodicTask(context, chrono::milliseconds(kTickPrecision)));

  expiry_task_->Start([this](int ticks) {
    DCHECK_GT(ticks, 0);
    this->expire_timer_.advance(ticks);
    DVLOG(3) << "Advancing expiry to " << this->expire_timer_.now();
  });

  return ec;
}

auto Channel::PresendChecks() -> error_code {
  if (!socket_->is_open()) {
    return asio::error::shut_down;
  }

  if (socket_->status()) {
    return socket_->status();
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

auto Channel::Send(uint32 deadline_msec, Envelope* envelope) -> future_code_t {
  DCHECK(read_fiber_.joinable()) << "Call Channel::Connect(), stupid.";
  DCHECK_GT(deadline_msec, 0);

  // ----
  fibers::promise<error_code> p;
  fibers::future<error_code> res = p.get_future();
  error_code ec = PresendChecks();

  if (ec) {
    p.set_value(ec);
    return res;
  }

  uint32_t ticks = (deadline_msec + kTickPrecision - 1) / kTickPrecision;
  std::unique_ptr<ExpiryEvent> ev(new ExpiryEvent(this));

  // We protect against Send thread vs IoContext thread data races.
  // Fibers inside IoContext thread do not have to protect against each other since
  // they do not cause data races. So we use lock_shared as "no-op" lock that only becomes
  // relevant if someone outside IoContext thread locks exclusively.
  // Also this lock allows multi-threaded access for Send operation.
  bool lock_exclusive = OutgoingBufLock();
  RpcId id = next_send_rpc_id_++;

  ev->set_id(id);
  base::Tick at = expire_timer_.schedule(ev.get(), ticks);
  DVLOG(2) << "Scheduled expiry at " << at << " for rpcid " << id;

  outgoing_buf_.emplace_back(SendItem(id, PendingCall{std::move(p), envelope}));
  outgoing_buf_.back().second.expiry_event = std::move(ev);
  outgoing_buf_size_.store(outgoing_buf_.size(), std::memory_order_relaxed);

  OutgoingBufUnlock(lock_exclusive);

  return res;
}

auto Channel::SendAndReadStream(Envelope* msg, MessageCallback cb) -> error_code {
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

  RpcId id = next_send_rpc_id_++;

  outgoing_buf_.emplace_back(SendItem(id, PendingCall{std::move(p), msg, std::move(cb)}));
  outgoing_buf_size_.store(outgoing_buf_.size(), std::memory_order_relaxed);

  OutgoingBufUnlock(exclusive);
  ec = future.get();

  return ec;
}

void Channel::ReadFiber() {
  CHECK(socket_->context().InContextThread());

  VLOG(1) << "Start ReadFiber on socket " << socket_->native_handle();
  this_fiber::properties<IoFiberProperties>().SetNiceLevel(1);

  while (socket_->is_open()) {
    error_code ec = ReadEnvelope();
    if (ec) {
      LOG_IF(WARNING, !IsExpectedFinish(ec))
          << "Error reading envelope " << ec << " " << ec.message();

      CancelPendingCalls(ec);
      // Required for few reasons:
      // 1. To preempt the fiber, otherwise it has busy loop in case socket returns the error
      //    preventing other fibers to run.
      // 2. To throttle cpu.
      this_fiber::sleep_for(10ms);
      continue;
    }
  }
  CancelPendingCalls(error_code{});
  VLOG(1) << "Finish ReadFiber on socket " << socket_->native_handle();
}


// TODO: To attach Flusher io context thread, similarly to server side.
void Channel::FlushFiber() {
  using namespace std::chrono_literals;
  CHECK(socket_->context().get_executor().running_in_this_thread());
  this_fiber::properties<IoFiberProperties>().SetNiceLevel(4);

  while (true) {
    this_fiber::sleep_for(300us);
    if (!socket_->is_open())
      break;

    if (outgoing_buf_size_.load(std::memory_order_acquire) == 0 || !send_mu_.try_lock())
      continue;
    VLOG(1) << "FlushFiber::FlushSendsGuarded";
    FlushSendsGuarded();
    outgoing_buf_size_.store(outgoing_buf_.size(), std::memory_order_release);

    send_mu_.unlock();  // releases the fence
  }
}

auto Channel::FlushSends() -> error_code {
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

auto Channel::FlushSendsGuarded() -> error_code {
  error_code ec;
  // This function runs only in IOContext thread. Therefore only
  if (outgoing_buf_.empty())
    return ec;

  ec = socket_->status();
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
  asio::write(*socket_, write_seq_, ec);
  if (ec) {
    // I do not know if we need to flush everything but I prefer doing it to make it simpler.
    CancelPendingCalls(ec);
    return ec;
  }

  return ec;
}

void Channel::ExpirePending(RpcId id) {
  DVLOG(1) << "Expire rpc id " << id;

  auto it = this->pending_calls_.find(id);
  if (it == this->pending_calls_.end()) {
    // TODO: there could be that the call is in outgoing_buf_ and we did not expiry it.
    return;
  }
  // The order is important to eliminate interrupts.
  EcPromise pr = std::move(it->second.promise);
  this->pending_calls_.erase(it);
  pr.set_value(asio::error::timed_out);
}

void Channel::CancelSentBufferGuarded(error_code ec) {
  std::vector<SendItem> tmp;

  buf_lock_.lock_shared();
  tmp.swap(outgoing_buf_);
  buf_lock_.unlock_shared();

  for (auto& item : tmp) {
    auto promise = std::move(item.second.promise);
    promise.set_value(ec);
  }
}

auto Channel::ReadEnvelope() -> error_code {
  Frame f;
  error_code ec = f.Read(socket_.get());
  if (ec)
    return ec;

  VLOG(2) << "Got rpc_id " << f.rpc_id << " from socket " << socket_->native_handle();

  auto it = pending_calls_.find(f.rpc_id);
  if (it == pending_calls_.end()) {
    // It might happens if for some reason we flushed pending_calls_ or the rpc has expired and
    //  the envelope reached us afterwards. We just consume it.
    VLOG(1) << "Unknown id " << f.rpc_id;

    Envelope envelope(f.header_size, f.letter_size);

    // ReadEnvelope is called via Channel::Apply, so no need to call it here.
    asio::read(*socket_, envelope.buf_seq(), ec);
    return ec;
  }

  // -- NO interrupt section begin
  PendingCall& call = it->second;
  Envelope* env = call.envelope;
  env->Resize(f.header_size, f.letter_size);
  bool is_stream = static_cast<bool>(call.cb);

  if (is_stream) {
    VLOG(1) << "Processing stream";
    asio::read(*socket_, env->buf_seq(), ec);
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

  asio::read(*socket_, env->buf_seq(), ec);
  promise.set_value(ec);

  return ec;
}

void Channel::HandleStreamResponse(RpcId rpc_id) {
  auto it = pending_calls_.find(rpc_id);
  if (it == pending_calls_.end()) {
    return;  // Might happen if pending_calls_ was cancelled when we read the envelope.
  }
  PendingCall& call = it->second;
  error_code ec = call.cb(*call.envelope);
  if (!ec)
    return;

  // eof - means successful finish of stream receival.
  if (ec == error::eof) {
    ec = system::error_code{};
  }

  // We finished processing the stream.
  // Keep the promise on the stack and erase from pending_calls_ first because
  // set_value might context switch and invalidate 'it'.
  auto promise = std::move(call.promise);
  pending_calls_.erase(it);
  promise.set_value(ec);
}

void Channel::CancelPendingCalls(error_code ec) {
  if (pending_calls_.empty())
    return;

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
