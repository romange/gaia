// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/rpc/rpc_server.h"

#include "base/logging.h"
#include "base/pod_array.h"

#include <boost/asio/buffer.hpp>
#include <boost/asio/completion_condition.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/write.hpp>
#include "util/asio/connection_handler.h"
#include "util/asio/accept_server.h"
#include "util/asio/asio_utils.h"
#include "util/asio/yield.h"
#include "util/rpc/frame_format.h"

namespace util {
namespace rpc {

using namespace boost;
using namespace system;
using std::string;
using fibers_ext::yield;

class RpcConnectionHandler : public ConnectionHandler {
 public:
  RpcConnectionHandler(asio::io_context* io_svc,  // not owned.
                       fibers::condition_variable* done,  // not owned
                       // owned by the instance.
                       ConnectionBridge* bridge);

  system::error_code HandleRequest() final override;

 private:
  base::PODArray<uint8_t> header_, letter_;
  std::unique_ptr<ConnectionBridge> bridge_;
};


RpcConnectionHandler::RpcConnectionHandler(asio::io_context* io_svc,
                                           fibers::condition_variable* done,
                                           ConnectionBridge* bridge)
    : ConnectionHandler(io_svc, done), bridge_(bridge) {
}

system::error_code RpcConnectionHandler::HandleRequest() {
  VLOG(2) << "RpcConnectionHandler " << socket_.remote_endpoint();

  rpc::Frame frame;
  system::error_code ec = frame.Read(&socket_);
  VLOG(1) << "Frame read " << ec;
  if (ec)
    return ec;

  if (frame.header_size == 0) {
    return errc::make_error_code(errc::protocol_error);
  }

  header_.resize(frame.header_size);
  letter_.resize(frame.letter_size);

  size_t sz;

  auto rbuf_seq = make_buffer_seq(header_, letter_);
  sz = asio::async_read(socket_, rbuf_seq, yield[ec]);
  if (ec) return ec;
  CHECK_EQ(sz, frame.header_size + frame.letter_size);

  Status status = bridge_->HandleEnvelope(frame.rpc_id, &header_, &letter_);
  if (!status.ok()) {
    return errc::make_error_code(errc::bad_message);
  }

  frame.header_size = header_.size();
  frame.letter_size = letter_.size();
  uint8_t buf[rpc::Frame::kMaxByteSize];
  auto fsz = frame.Write(buf);

  auto wbuf_seq = make_buffer_seq(asio::buffer(buf, fsz), header_, letter_);
  sz = asio::async_write(socket_, wbuf_seq, yield[ec]);
  if (ec) return ec;
  CHECK_EQ(sz, frame.header_size + frame.letter_size + fsz);

  return system::error_code{};
}

Server::Server(unsigned short port) : port_(port) {

}

Server::~Server() {
}

void Server::BindTo(ServiceInterface* iface) {
  cf_ = [iface](io_context* cntx, fibers::condition_variable* done) -> ConnectionHandler* {
    ConnectionBridge* bridge = iface->CreateConnectionBridge();
    return new RpcConnectionHandler(cntx, done, bridge);
  };
}

void Server::Run(IoContextPool* pool) {
  CHECK(cf_) << "Must call BindTo before running Run(...)";

  acc_server_.reset(new AcceptServer(port_, pool, cf_));
  acc_server_->Run();
  port_ = acc_server_->port();
}

void Server::Stop() {
  CHECK(acc_server_);
  acc_server_->Stop();
}

void Server::Wait() {
  acc_server_->Wait();
  cf_ = nullptr;
}

}  // namespace rpc
}  // namespace util
