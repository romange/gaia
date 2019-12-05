// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "util/rpc/impl/rpc_conn_handler.h"

#include <boost/asio/read.hpp>
#include <boost/asio/write.hpp>

#include "base/flags.h"
#include "base/logging.h"

#include "util/asio/asio_utils.h"
#include "util/asio/io_context.h"

namespace util {
namespace rpc {

using namespace std::chrono_literals;

namespace {}  // namespace

using asio::ip::tcp;
using fibers_ext::yield;

constexpr size_t kRpcPoolSize = 32;

RpcConnectionHandler::RpcConnectionHandler(ConnectionBridge* bridge, IoContext* context)
    : ConnectionHandler(context), bridge_(bridge), rpc_items_(kRpcPoolSize) {
  use_flusher_fiber_ = true;
}

RpcConnectionHandler::~RpcConnectionHandler() {
  bridge_->Join();

  outgoing_buf_.clear_and_dispose([this](RpcItem* i) { rpc_items_.Release(i); });
}

void RpcConnectionHandler::FlushWrites() {
  if (socket_->is_open() && !outgoing_buf_.empty()) {
    req_flushes_ += uint64_t(FlushWritesInternal());
  }
}

void RpcConnectionHandler::OnOpenSocket() {
  VLOG(1) << "OnOpenSocket: " << socket_->native_handle();

  bridge_->InitInThread();
}

void RpcConnectionHandler::OnCloseSocket() {
  VLOG(1) << "OnCloseSocket: Before flush fiber join " << socket_->native_handle();
  DCHECK(io_context_.InContextThread());
}

system::error_code RpcConnectionHandler::HandleRequest() {
  VLOG(2) << "HandleRequest " << socket_->is_open() << " / "
          << (socket_->is_open() ? socket_->remote_endpoint(ec_) : tcp::endpoint());

  if (ec_)
    return ec_;

  rpc::Frame frame;
  ec_ = frame.Read(&socket_.value());
  if (ec_) {
    return ec_;
  }

  DCHECK_NE(-1, socket_->native_handle());

  if (rpc_items_.empty() && !outgoing_buf_.empty()) {
    req_flushes_ += FlushWritesInternal();
  }

  // We use item for reading the envelope.
  auto item_ptr = rpc_items_.make_unique();

  Envelope* envelope = &item_ptr->envelope;
  envelope->Resize(frame.header_size, frame.letter_size);
  auto rbuf_seq = item_ptr->buf_seq();
  asio::read(*socket_, rbuf_seq, ec_);
  if (ec_) {
    VLOG(1) << "async_read " << ec_ << " /" << socket_->native_handle();
    return ec_;
  }
  DCHECK_NE(-1, socket_->native_handle());

  // To support streaming we have this writer that creq_flushes_an write multiple envelopes per
  // single rpc request. We pass captures by value to allow asynchronous invocation
  // of ConnectionBridge::HandleEnvelope. We move writer object into HandleEnvelope,
  // thus it will be responsible to own it until the handler finishes.
  // Please note that writer changes the value of 'item' field (it's mutable),
  // so only for the first outgoing envelope it uses the same RpcItem used for reading the data
  // to reduce allocations.
  auto writer = [rpc_id = frame.rpc_id, item = item_ptr.release(), this](Envelope&& env) mutable {
    RpcItem* next = item ? item : rpc_items_.Get();

    next->envelope = std::move(env);
    next->id = rpc_id;
    outgoing_buf_.push_back(*next);
    item = nullptr;
  };

  // Might by asynchronous, depends on the bridge_.
  bridge_->HandleEnvelope(frame.rpc_id, envelope, std::move(writer));

  return ec_;
}

bool RpcConnectionHandler::FlushWritesInternal() {
  // Serves as critical section. We can not allow interleaving writes into the socket.
  // If another fiber flushes - we just exit without blocking.
  std::unique_lock<fibers::mutex> ul(wr_mu_, std::try_to_lock_t{});
  if (!ul || outgoing_buf_.empty() || !socket_->is_open())
    return false;

  VLOG(2) << "FlushWritesGuarded: " << outgoing_buf_.size();
  size_t count = outgoing_buf_.size();
  write_seq_.resize(count * 3);
  frame_buf_.resize(count);

  ItemList tmp;
  size_t item_index = 0;
  for (RpcItem& item : outgoing_buf_) {  // iterate over intrusive list.
    Frame f(item.id, item.envelope.header.size(), item.envelope.letter.size());

    uint8_t* buf = frame_buf_[item_index].data();
    size_t frame_sz = f.Write(buf);
    write_seq_[3 * item_index] = asio::buffer(buf, frame_sz);
    write_seq_[3 * item_index + 1] = asio::buffer(item.envelope.header);
    write_seq_[3 * item_index + 2] = asio::buffer(item.envelope.letter);
    ++item_index;
  }
  tmp.swap(outgoing_buf_);

  size_t write_sz = asio::write(*socket_, write_seq_, ec_);

  // We should use clear_and_dispose to delete items safely while unlinking them from tmp.
  tmp.clear_and_dispose([this](RpcItem* i) { rpc_items_.Release(i); });

  VLOG(2) << "Wrote " << count << " requests with " << write_sz << " bytes";
  return true;
}

}  // namespace rpc
}  // namespace util
