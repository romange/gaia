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
  RpcConnectionHandler(const RpcServiceDescriptor* descr,  asio::io_context* io_svc,
                       fibers::condition_variable* done);

  system::error_code HandleRequest() final override;

 private:
  base::PODArray<uint8_t> control_, payload_;
  const RpcServiceDescriptor* descr_ = nullptr;
};


RpcConnectionHandler::RpcConnectionHandler(const RpcServiceDescriptor* descr,
                                           asio::io_context* io_svc,
                                           fibers::condition_variable* done)
    : ConnectionHandler(io_svc, done), descr_(descr) {
}

system::error_code RpcConnectionHandler::HandleRequest() {
  rpc::Frame frame;
  system::error_code ec = frame.Read(&socket_);
  if (ec)
    return ec;

  if (frame.control_size == 0) {
    return errc::make_error_code(errc::protocol_error);
  }

  control_.resize(frame.control_size);

  asio::async_read(socket_, asio::buffer(control_.data(), frame.control_size),
                   asio::transfer_exactly(frame.control_size), yield[ec]);
  if (ec) return ec;

  asio::async_read(socket_, asio::buffer(payload_.data(), frame.msg_size),
                   asio::transfer_exactly(frame.msg_size), yield[ec]);
  if (ec) return ec;


  return ec;
}

RpcServer::RpcServer(unsigned short port) : port_(port) {

}

RpcServer::~RpcServer() {

}


void RpcServer::Run(IoContextPool* pool) {
  acc_server_.reset(new AcceptServer(port_, pool, nullptr));
  acc_server_->Run();
}

void RpcServer::Wait() {

}

}  // namespace util
