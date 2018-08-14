// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/rpc/rpc_server.h"

#include "base/logging.h"
#include "base/pod_array.h"

#include <boost/asio/buffer.hpp>
#include <boost/asio/completion_condition.hpp>
#include <boost/asio/read.hpp>

#include "util/asio/connection_handler.h"
#include "util/asio/accept_server.h"
#include "util/asio/yield.h"

#include "util/rpc/frame_format.h"

namespace util {
using namespace boost;
using namespace system;
using std::string;
using fibers_ext::yield;

class RpcConnectionHandler : public ConnectionHandler {
 public:
  RpcConnectionHandler(asio::io_context* io_svc,  // not owned.
                       fibers::condition_variable* done,  // not owned
                       // owned by the instance.
                       RpcConnectionBridge* bridge);

  system::error_code HandleRequest() final override;

 private:
  base::PODArray<uint8_t> header_, letter_;
  std::unique_ptr<RpcConnectionBridge> bridge_;
};


RpcConnectionHandler::RpcConnectionHandler(asio::io_context* io_svc,
                                           fibers::condition_variable* done,
                                           RpcConnectionBridge* bridge)
    : ConnectionHandler(io_svc, done), bridge_(bridge) {
}

system::error_code RpcConnectionHandler::HandleRequest() {
  rpc::Frame frame;
  system::error_code ec = frame.Read(&socket_);
  if (ec)
    return ec;

  if (frame.header_size == 0) {
    return errc::make_error_code(errc::protocol_error);
  }

  header_.resize(frame.header_size);
  size_t sz;

  std::array<asio::mutable_buffer, 2> buffers =
    {asio::mutable_buffer{header_.data(), frame.header_size},
     asio::mutable_buffer{letter_.data(), frame.letter_size}};

  sz = asio::async_read(socket_, buffers, yield[ec]);
  if (ec) return ec;
  CHECK_EQ(sz, frame.header_size + frame.letter_size);

  Status status = bridge_->HandleEnvelope(&header_, &letter_);
  if (!status.ok()) {
    return errc::make_error_code(errc::bad_message);
  }
  return system::error_code{};
}

RpcServer::RpcServer(unsigned short port) : port_(port) {

}

RpcServer::~RpcServer() {

}

void RpcServer::BindTo(RpcServiceInterface* iface) {
  cf_ = [iface](io_context* cntx, fibers::condition_variable* done) -> RpcConnectionHandler* {
    RpcConnectionBridge* bridge = iface->CreateConnectionBridge();
    return new RpcConnectionHandler(cntx, done, bridge);
  };
}

void RpcServer::Run(IoContextPool* pool) {
  CHECK(cf_) << "Must call BindTo before running Run(...)";

  acc_server_.reset(new AcceptServer(port_, pool, cf_));
  acc_server_->Run();
}

void RpcServer::Wait() {
  acc_server_->Wait();
  cf_ = nullptr;
}

}  // namespace util
