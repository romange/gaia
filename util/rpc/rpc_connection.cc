// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "util/rpc/rpc_connection.h"

#include <boost/asio/buffer.hpp>
#include <boost/asio/completion_condition.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/write.hpp>

#include "base/flags.h"
#include "base/logging.h"

#include "util/asio/accept_server.h"
#include "util/asio/asio_utils.h"
#include "util/asio/yield.h"
#include "util/rpc/impl/rpc_conn_handler.h"

DEFINE_int32(rpc_server_buffer_size, 4096, "");

namespace util {
namespace rpc {

using namespace system;
using asio::ip::tcp;
using boost::asio::io_context;
using fibers_ext::yield;
using std::string;

constexpr size_t kRpcPoolSize = 32;

RpcConnectionHandler::RpcConnectionHandler(ConnectionBridge* bridge)
    : bridge_(bridge), rpc_items_(kRpcPoolSize) {
  if (FLAGS_rpc_server_buffer_size > 0) {
    buf_read_sock_.reset(
        new BufferedReadAdaptor<tcp::socket>(*socket_, FLAGS_rpc_server_buffer_size));
  }
}

RpcConnectionHandler::~RpcConnectionHandler() {
  VLOG_IF(1, buf_read_sock_) << "Saved " << buf_read_sock_->saved() << " bytes";
  if (flush_fiber_.joinable()) {
    flush_fiber_.join();
  }

  for (auto& item : outgoing_buf_) {
    rpc_items_.Release(&item);
  }
}

void RpcConnectionHandler::OnOpenSocket() {
  flush_fiber_ = fibers::fiber(&RpcConnectionHandler::FlushFiber, this);
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
  if (buf_read_sock_) {
    ec_ = frame.Read(buf_read_sock_.get());
  } else {
    ec_ = frame.Read(&socket_.value());
  }
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
  if (buf_read_sock_) {
    ec_ = buf_read_sock_->Read(rbuf_seq);
  } else {
    asio::async_read(*socket_, rbuf_seq, yield[ec_]);
  }
  if (ec_) {
    VLOG(1) << "async_read " << ec_ << " /" << socket_->native_handle();
    return ec_;
  }
  DCHECK_NE(-1, socket_->native_handle());

  // To support streaming we have this writer that can write multiple envelopes per
  // single rpc request.
  bool first_time = true;
  auto writer = [&](Envelope&& env) {
    RpcItem* next = first_time ? item : rpc_items_.Get();

    next->envelope = std::move(env);
    next->id = frame.rpc_id;
    outgoing_buf_.push_back(*next);
    first_time = false;
  };

  bridge_->HandleEnvelope(frame.rpc_id, &item->envelope, std::move(writer));

  if (ShouldFlush()) {
    std::lock_guard<fibers::mutex> l(wr_mu_);
    FlushWritesGuarded();
  }
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
  for (auto& item : tmp) {
    rpc_items_.Release(&item);
  }

  VLOG(1) << "Wrote " << count << " requests with " << write_sz << " bytes";
}

void RpcConnectionHandler::FlushFiber() {
  using namespace std::chrono_literals;
  CHECK(socket_->get_executor().running_in_this_thread());
  VLOG(1) << "RpcConnectionHandler::FlushFiber";

  while (true) {
    this_fiber::sleep_for(100us);
    if (!is_open_ || !socket_->is_open())
      break;

    if (outgoing_buf_.empty() || !wr_mu_.try_lock())
      continue;
    VLOG(1) << "FlushFiber::IFlushWritesGuarded";
    FlushWritesGuarded();
    wr_mu_.unlock();
  }
  VLOG(1) << "RpcConnectionHandler::FlushFiberExit";
}

inline bool RpcConnectionHandler::ShouldFlush() {
  return rpc_items_.empty() && !outgoing_buf_.empty();
}

ConnectionHandler* ServiceInterface::NewConnection() {
  ConnectionBridge* bridge = CreateConnectionBridge();
  return new RpcConnectionHandler(bridge);
}

}  // namespace rpc
}  // namespace util
