// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "util/rpc/impl/rpc_conn_handler.h"

#include <boost/asio/read.hpp>
#include <boost/asio/write.hpp>

#include "base/flags.h"
#include "base/logging.h"

#include "util/asio/asio_utils.h"

DEFINE_uint32(rpc_server_buffer_size, 4096, "");

namespace util {
namespace rpc {

using asio::ip::tcp;
using fibers_ext::yield;

constexpr size_t kRpcPoolSize = 32;

RpcConnectionHandler::RpcConnectionHandler(ConnectionBridge* bridge)
    : bridge_(bridge), buf_read_sock_(
      new BufferedReadAdaptor<tcp::socket>(*socket_, FLAGS_rpc_server_buffer_size)),
      rpc_items_(kRpcPoolSize)  {
}

RpcConnectionHandler::~RpcConnectionHandler() {
  VLOG_IF(1, buf_read_sock_) << "Saved " << buf_read_sock_->saved() << " bytes";
  if (flush_fiber_.joinable()) {
    flush_fiber_.join();
  }
  bridge_->Join();

  outgoing_buf_.clear_and_dispose([this](RpcItem* i) { rpc_items_.Release(i);});
}

void RpcConnectionHandler::OnOpenSocket() {
  // TODO: To make flush_fiber_ thread-local, handling all the subscribed rpc connection handlers.
  flush_fiber_ = fibers::fiber(&RpcConnectionHandler::FlushFiber, this);
  bridge_->InitInThread();
}

void RpcConnectionHandler::OnCloseSocket() {
  // CHECK(socket_->get_executor().running_in_this_thread());
  VLOG(1) << "Before flush fiber join " << socket_->native_handle();
  if (flush_fiber_.joinable()) {
    flush_fiber_.join();
  }
  VLOG(1) << "After flush fiber join";
}

system::error_code RpcConnectionHandler::HandleRequest() {
  VLOG(2) << "HandleRequest " << socket_->is_open() << " / "
          << (socket_->is_open() ? socket_->remote_endpoint(ec_) : tcp::endpoint());

  if (ec_)
    return ec_;

  rpc::Frame frame;
  ec_ = frame.Read(buf_read_sock_.get());
  if (ec_) {
    return ec_;
  }

  DCHECK_NE(-1, socket_->native_handle());

  if (ShouldFlush()) {
    std::lock_guard<fibers::mutex> l(wr_mu_);
    FlushWritesGuarded();
  }

  RpcItem* item = rpc_items_.Get();

  item->envelope.Resize(frame.header_size, frame.letter_size);
  auto rbuf_seq = item->buf_seq();
  ec_ = buf_read_sock_->Read(rbuf_seq);
  if (ec_) {
    VLOG(1) << "async_read " << ec_ << " /" << socket_->native_handle();
    return ec_;
  }
  DCHECK_NE(-1, socket_->native_handle());

  struct WriterData {
    RpcId rpc_id;
    RpcItem* item;
  };

  // To support streaming we have this writer that can write multiple envelopes per
  // single rpc request. We pass additional data by value to allow asynchronous invocation
  // of ConnectionBridge::HandleEnvelope. We move writer into it, this it will be responsible to
  // own it until the handler finishes.
  auto writer = [data = WriterData{frame.rpc_id, item}, this] (Envelope&& env) mutable {
    RpcItem* next = data.item ? data.item : rpc_items_.Get();

    next->envelope = std::move(env);
    next->id = data.rpc_id;
    outgoing_buf_.push_back(*next);
    data.item = nullptr;
  };

  bridge_->HandleEnvelope(frame.rpc_id, &item->envelope, std::move(writer));

  return ec_;
}

void RpcConnectionHandler::FlushWritesGuarded() {
  if (outgoing_buf_.empty() || !socket_->is_open())
    return;
  VLOG(2) << "FlushWritesGuarded: " << outgoing_buf_.size();
  size_t count = outgoing_buf_.size();
  write_seq_.resize(count * 3);
  frame_buf_.resize(count);
  ItemList tmp;

  size_t item_index = 0;
  for (RpcItem& item : outgoing_buf_) {
    Frame f(item.id, item.envelope.header.size(), item.envelope.letter.size());

    uint8_t* buf = frame_buf_[item_index].data();
    size_t frame_sz = f.Write(buf);
    write_seq_[3 * item_index] = asio::buffer(buf, frame_sz);
    write_seq_[3 * item_index + 1] = asio::buffer(item.envelope.header);
    write_seq_[3 * item_index + 2] = asio::buffer(item.envelope.letter);
    ++item_index;
  }
  tmp.swap(outgoing_buf_);
  size_t write_sz = asio::async_write(*socket_, write_seq_, yield[ec_]);

  // We should use clear_and_dispose to delete items safely while unlinking them from tmp.
  tmp.clear_and_dispose([this](RpcItem* i) { rpc_items_.Release(i);});

  VLOG(2) << "Wrote " << count << " requests with " << write_sz << " bytes";
}

void RpcConnectionHandler::FlushFiber() {
  using namespace std::chrono_literals;
  CHECK(socket_->get_executor().running_in_this_thread());
  VLOG(1) << "RpcConnectionHandler::FlushFiber";

  // TODO: To save redunant spins
  // we could reimplement this with periodict task and condition_variable.
  // For each rpc connection we setup a Flush fiber which polls. That means that
  // if we have 1000 connections we poll 1000 times * this loop frequency.
  // It makes more sense to have a single poller per thread that awaken flush fibers if needed.
  while (true) {
    this_fiber::sleep_for(300us);
    if (!is_open_ || !socket_->is_open())
      break;

    if (outgoing_buf_.empty() || !wr_mu_.try_lock())
      continue;
    VLOG(2) << "FlushFiber::IFlushWritesGuarded";
    FlushWritesGuarded();
    wr_mu_.unlock();
  }
  VLOG(1) << "RpcConnectionHandler::FlushFiberExit";
}

inline bool RpcConnectionHandler::ShouldFlush() {
  return rpc_items_.empty() && !outgoing_buf_.empty();
}

}  // namespace rpc
}  // namespace util
