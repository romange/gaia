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

DEFINE_uint32(rpc_client_queue_size, 16,
              "The size of the outgoing batch queue that contains envelopes waiting to send.");

using namespace boost;
using asio::ip::tcp;

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
}

void ClientBase::Shutdown() {
  channel_.Shutdown();
}

auto ClientBase::Connect(uint32_t ms) -> error_code {
  CHECK(!read_fiber_.joinable());
  error_code ec = channel_.Connect(ms);

  read_fiber_ = fibers::fiber(&ClientBase::ReadFiber, this);
  flush_fiber_ = fibers::fiber(&ClientBase::FlushFiber, this);
  return ec;
}

auto ClientBase::PresendChecks() -> error_code {
  if (channel_.is_shut_down()) {
    return asio::error::shut_down;
  }

  if (channel_.status()) {
    return channel_.status();
  }

  if (pending_calls_.size() >= FLAGS_rpc_client_pending_limit) {
    return asio::error::no_buffer_space;
  }

  error_code ec;

  if (outgoing_buf_.size() > FLAGS_rpc_client_queue_size) {
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

  RpcId id = rpc_id_++;
  auto emplace_res = pending_calls_.emplace(id, PendingCall{std::move(p), envelope});
  CHECK(emplace_res.second);

  outgoing_buf_.emplace_back(id, envelope);

  return res;
}

void ClientBase::ReadFiber() {
  CHECK(channel_.socket().get_executor().running_in_this_thread());

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
    ec = channel_.Apply(do_not_lock, [this] { return this->ReadEnvelope(); });
  }
  CancelPendingCalls(ec);
  VLOG(1) << "Finish ReadFiber on socket " << channel_.handle();
}

void ClientBase::FlushFiber() {
  using namespace std::chrono_literals;
  CHECK(channel_.socket().get_executor().running_in_this_thread());

  while (true) {
    this_fiber::sleep_for(100us);
    if (channel_.is_shut_down())
      break;

    if (outgoing_buf_.empty() || !send_mu_.try_lock())
      continue;
    VLOG(1) << "FlushFiber::FlushSendsGuarded";
    FlushSendsGuarded();
    send_mu_.unlock();
  }
}

auto ClientBase::FlushSends() -> error_code {
  std::lock_guard<fibers::mutex> guard(send_mu_);

  error_code ec;
  while (outgoing_buf_.size() > FLAGS_rpc_client_queue_size) {
    ec = FlushSendsGuarded();
  }
  return ec;  // Return the last known status code.
}

auto ClientBase::FlushSendsGuarded() -> error_code {
  error_code ec;
  if (outgoing_buf_.empty())
    return ec;

  if ((ec = channel_.status())) {
    return CancelSentBufferGuarded(ec);
  }

  size_t count = outgoing_buf_.size();
  write_seq_.resize(count * 3);

  for (size_t i = 0; i < count; ++i) {
    auto& item = outgoing_buf_[i];
    Frame f(item.rpc_id, item.envelope->header.size(), item.envelope->letter.size());
    size_t sz = f.Write(item.frame_buf);

    write_seq_[3 * i] = asio::buffer(item.frame_buf, sz);
    write_seq_[3 * i + 1] = asio::buffer(item.envelope->header);
    write_seq_[3 * i + 2] = asio::buffer(item.envelope->letter);
  }

  // Interrupt point during which outgoing_buf_ could grow.
  // We do not lock because this function is the only one that writes into channel and it's
  // guarded by send_mu_.
  ec = channel_.Write(do_not_lock, write_seq_);
  if (ec) {
    return CancelSentBufferGuarded(ec);
  }

  outgoing_buf_.erase(outgoing_buf_.begin(), outgoing_buf_.begin() + count);
  return ec;
}

auto ClientBase::CancelSentBufferGuarded(error_code ec) -> error_code {
  std::vector<SendItem> tmp;
  tmp.swap(outgoing_buf_);

  for (const auto& item : tmp) {
    auto it = pending_calls_.find(item.rpc_id);
    CHECK(it != pending_calls_.end());
    auto promise = std::move(it->second.promise);
    pending_calls_.erase(it);
    promise.set_value(ec);
  }
  return ec;
}

auto ClientBase::ReadEnvelope() -> error_code {
  Frame f;
  error_code ec = f.Read(&br_);
  if (ec)
    return ec;

  VLOG(2) << "Got rpc_id " << f.rpc_id << " from socket " << channel_.handle();

  auto it = pending_calls_.find(f.rpc_id);
  if (it == pending_calls_.end()) {
    LOG(WARNING) << "Unknown id " << f.rpc_id;
    Envelope envelope(f.header_size, f.letter_size);
    auto rbuf_seq = envelope.buf_seq();
    ec = channel_.Apply(do_not_lock, [this, &rbuf_seq] { return br_.Read(rbuf_seq); });
    return ec;
  }

  PendingCall& call = it->second;
  Envelope* env = call.envelope;
  env->Resize(f.header_size, f.letter_size);
  auto promise = std::move(call.promise);

  // We erase before reading from the socket/setting promise because pending_calls_ might change
  // during IO and 'it' will be invalidated.
  pending_calls_.erase(it);
  ec = channel_.Apply(do_not_lock, [this, &call] { return br_.Read(call.envelope->buf_seq()); });
  promise.set_value(ec);

  return ec;
}

void ClientBase::CancelPendingCalls(error_code ec) {
  PendingMap tmp;
  tmp.swap(pending_calls_);

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
