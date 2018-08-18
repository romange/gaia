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

namespace rpc {

class ConnectionBridge {
 public:
  virtual ~ConnectionBridge() {}

  // header and letter are input/output parameters.
  // HandleEnvelope reads first the input and if everything is parsed fine, it sends
  // back another header, letter pair.
  virtual Status HandleEnvelope(base::PODArray<uint8_t>* header,
                                base::PODArray<uint8_t>* letter) = 0;
};

class ServiceInterface {
 public:
  virtual ~ServiceInterface() {}

  // A factory method creating a handler that should handles requests for a single connection.
  // The ownership over handler is passed to the caller.
  virtual ConnectionBridge* CreateConnectionBridge() = 0;
};

class Server {
 public:
  explicit Server(unsigned short port);
  ~Server();

  void BindTo(ServiceInterface* iface);

  void Run(IoContextPool* pool);

  void Stop();

  void Wait();
  uint16_t port() const { return port_; }

 private:
  uint16_t port_ = 0;
  std::unique_ptr<AcceptServer> acc_server_;
  AcceptServer::ConnectionFactory cf_;
};
}  // namespace rpc

}  // namespace util
