// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/rpc/rpc_connection.h"

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
#include "util/rpc/frame_format.h"

namespace util {
namespace rpc {

using namespace boost;
using namespace system;
using boost::asio::io_context;
using fibers_ext::yield;
using std::string;
using asio::ip::tcp;



class RpcConnectionHandler : public ConnectionHandler {
 public:
  // bridge is owned by RpcConnectionHandler instance.
  RpcConnectionHandler(ConnectionBridge* bridge);
  ~RpcConnectionHandler();

  system::error_code HandleRequest() final override;

 private:
  BufferType header_, letter_;
  uint64_t last_rpc_id_ = 0;
  std::unique_ptr<ConnectionBridge> bridge_;
  BufferedSocketReadAdaptor<tcp::socket> buf_read_sock_;
};

RpcConnectionHandler::RpcConnectionHandler(ConnectionBridge* bridge)
    : bridge_(bridge), buf_read_sock_(*socket_, 1024) {
}

RpcConnectionHandler::~RpcConnectionHandler() {
  LOG(INFO) << "Saved " << buf_read_sock_.saved() << " bytes";
}

#define BUF_READ 0

system::error_code RpcConnectionHandler::HandleRequest() {
  VLOG(2) << "HandleRequest " << socket_->remote_endpoint() << "/" << socket_->is_open();

  rpc::Frame frame;
#if (BUF_READ)
  system::error_code ec = frame.Read(&buf_read_sock_);
#else
  system::error_code ec = frame.Read(&socket_.value());
#endif

  if (ec) {
    return ec;
  }
  DCHECK_NE(-1, socket_->native_handle());
  DCHECK_LT(last_rpc_id_, frame.rpc_id);
  last_rpc_id_ = frame.rpc_id;

  header_.resize(frame.header_size);
  letter_.resize(frame.letter_size);

  auto rbuf_seq = make_buffer_seq(header_, letter_);
#if (BUF_READ)
  ec = buf_read_sock_.Read(rbuf_seq);
#else
  asio::async_read(*socket_, rbuf_seq, yield[ec]);
#endif

  if (ec) {
    VLOG(1) << "async_read " << ec << " /" << socket_->native_handle();
    return ec;
  }
  DCHECK_NE(-1, socket_->native_handle());

  Status status = bridge_->HandleEnvelope(frame.rpc_id, &header_, &letter_);
  if (!status.ok()) {
    return errc::make_error_code(errc::bad_message);
  }

  frame.header_size = header_.size();
  frame.letter_size = letter_.size();
  uint8_t buf[rpc::Frame::kMaxByteSize];
  auto fsz = frame.Write(buf);

  auto wbuf_seq = make_buffer_seq(asio::buffer(buf, fsz), header_, letter_);
  VLOG(2) << "Writing frame " << frame.rpc_id;
  size_t sz = asio::async_write(*socket_, wbuf_seq, yield[ec]);
  if (ec) {
    VLOG(1) << "async_write " << ec;
    return ec;
  }

  CHECK_EQ(sz, frame.total_size() + fsz);

  return system::error_code{};
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
