// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "util/rpc/rpc_connection.h"

#include "base/flags.h"
#include "base/logging.h"
#include "base/pod_array.h"

#include <boost/asio/buffer.hpp>
#include <boost/asio/completion_condition.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/write.hpp>
#include "util/asio/accept_server.h"
#include "util/asio/asio_utils.h"
#include "util/asio/connection_handler.h"
#include "util/asio/yield.h"
// #include "util/fibers_ext.h"
#include "util/rpc/frame_format.h"


namespace util {
namespace rpc {

using namespace boost;
using namespace system;
using asio::ip::tcp;
using boost::asio::io_context;
using fibers_ext::yield;
using std::string;

DEFINE_int32(rpc_server_buffer_size, -1, "");

constexpr size_t kRpcItemSize = 16;

class RpcConnectionHandler : public ConnectionHandler {
 public:
  // bridge is owned by RpcConnectionHandler instance.
  RpcConnectionHandler(ConnectionBridge* bridge);
  ~RpcConnectionHandler();

  system::error_code HandleRequest() final override;

 private:
  void FlushWritesGuarded();
  void FlushFiber();
  void OnOpenSocket() final;
  void OnCloseSocket() final;

  uint64_t last_rpc_id_ = 0;
  std::unique_ptr<ConnectionBridge> bridge_;
  std::unique_ptr<BufferedSocketReadAdaptor<tcp::socket>> buf_read_sock_;

  struct RpcItem {
    BufferType header, letter;
    uint8_t frame_buf[rpc::Frame::kMaxByteSize];
    size_t frame_sz = 0;

    void SyncFrame(uint64_t rpc_id) {
      Frame frame(rpc_id, header.size(), letter.size());
      frame_sz = frame.Write(frame_buf);
    }
  };

  system::error_code ec_;
  std::unique_ptr<RpcItem[]> item_storage_;
  std::vector<RpcItem*> avail_item_, outgoing_buf_;

  fibers::mutex wr_mu_;
  std::vector<asio::const_buffer> write_seq_;

  fibers::fiber flush_fiber_;
};


RpcConnectionHandler::RpcConnectionHandler(ConnectionBridge* bridge)
    : bridge_(bridge), item_storage_(new RpcItem[kRpcItemSize]), avail_item_(kRpcItemSize) {
  if (FLAGS_rpc_server_buffer_size > 0) {
    buf_read_sock_.reset(
        new BufferedSocketReadAdaptor<tcp::socket>(*socket_, FLAGS_rpc_server_buffer_size));
  }
  for (size_t i = 0; i < kRpcItemSize; ++i) {
    avail_item_[i] = item_storage_.get() + i;
  }
}

RpcConnectionHandler::~RpcConnectionHandler() {
  VLOG_IF(1, buf_read_sock_) << "Saved " << buf_read_sock_->saved() << " bytes";
  if (flush_fiber_.joinable()) {
    flush_fiber_.join();
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
  DCHECK_LT(last_rpc_id_, frame.rpc_id);
  last_rpc_id_ = frame.rpc_id;

  if (avail_item_.empty()) {
    std::lock_guard<fibers::mutex> l(wr_mu_);
    FlushWritesGuarded();
    if (ec_)
      return ec_;
  }
  DCHECK(!avail_item_.empty());
  RpcItem* item = avail_item_.back();
  avail_item_.pop_back();
  DCHECK(item);

  item->header.resize(frame.header_size);
  item->letter.resize(frame.letter_size);

  auto rbuf_seq = make_buffer_seq(item->header, item->letter);
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

  Status status = bridge_->HandleEnvelope(frame.rpc_id, &item->header, &item->letter);
  if (!status.ok()) {
    return errc::make_error_code(errc::bad_message);
  }

  item->SyncFrame(frame.rpc_id);
  outgoing_buf_.push_back(item);

  return system::error_code{};
}

void RpcConnectionHandler::FlushWritesGuarded() {
  if (outgoing_buf_.empty() || !socket_->is_open())
    return;
  VLOG(2) << "FlushWritesGuarded: " << outgoing_buf_.size();
  size_t count = outgoing_buf_.size();
  write_seq_.resize(count * 3);
  for (size_t i = 0; i < count; ++i) {
    RpcItem* item = outgoing_buf_[i];
    DCHECK_LE(item->frame_sz, sizeof(item->frame_buf));

    write_seq_[3 * i] = asio::buffer(item->frame_buf, item->frame_sz);
    write_seq_[3 * i + 1] = asio::buffer(item->header.data(), item->header.size());
    write_seq_[3 * i + 2] = asio::buffer(item->letter.data(), item->letter.size());
  }

  size_t write_sz = asio::async_write(*socket_, write_seq_, yield[ec_]);
  if (ec_) {
    return;
  }
  VLOG(2) << "FlushWritesWrote " << write_sz << " bytes";
  for (size_t i = 0; i < count; ++i) {
    RpcItem* item = outgoing_buf_[i];
    avail_item_.push_back(item);
  }
  outgoing_buf_.erase(outgoing_buf_.begin(), outgoing_buf_.begin() + count);
}

void RpcConnectionHandler::FlushFiber() {
  using namespace std::chrono_literals;
  CHECK(socket_->get_executor().running_in_this_thread());

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
}

uint16_t ServiceInterface::Listen(uint16_t port, AcceptServer* acc_server) {
  AcceptServer::ConnectionFactory cf = [this]() -> ConnectionHandler* {
    ConnectionBridge* bridge = CreateConnectionBridge();
    return new RpcConnectionHandler(bridge);
  };
  return acc_server->AddListener(port, std::move(cf));
}

}  // namespace rpc
}  // namespace util
