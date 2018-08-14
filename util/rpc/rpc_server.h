// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <memory>

#include <google/protobuf/message.h>

#include "base/pod_array.h"
#include "util/asio/accept_server.h"
#include "util/status.h"
#include "strings/stringpiece.h"

namespace util {

class AcceptServer;
class IoContextPool;


class RpcConnectionBridge {
 public:
  virtual ~RpcConnectionBridge() {}

  // header and letter are input/output parameters.
  // HandleEnvelope reads first the input and if everything is parsed fine, it sends
  // back another header, letter pair.
  virtual Status HandleEnvelope(base::PODArray<uint8_t>* header,
                                base::PODArray<uint8_t>* letter) = 0;
};

class RpcServiceInterface {
 public:
  virtual ~RpcServiceInterface() {};

  // A factory method creating a handler that should handles requests for a single connection.
  // The ownership over handler is passed to the caller.
  virtual RpcConnectionBridge* CreateConnectionBridge() = 0;
};

class RpcServer {
 public:
  RpcServer(unsigned short port);
  ~RpcServer();

  void BindTo(RpcServiceInterface* iface);

  void Run(IoContextPool* pool);
  void Wait();

 protected:

 private:
  unsigned short port_ = 0;
  std::unique_ptr<AcceptServer> acc_server_;
  AcceptServer::ConnectionFactory cf_;
};

}  // namespace util
