// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <memory>

#include <google/protobuf/message.h>

#include "util/asio/accept_server.h"
#include "util/rpc/rpc_envelope.h"
#include "util/status.h"
#include "strings/stringpiece.h"

namespace util {

// RPC-Server side part.

class AcceptServer;

namespace rpc {

// ConnectionBridge is responsible to abstract higher level server-app logic and to provide
// an interface that allows to map Envelope to ServiceInterface methods.
class ConnectionBridge {
 public:
  typedef std::function<void(Envelope&&)> EnvelopeWriter;

  virtual ~ConnectionBridge() {}

  // header and letter are input/output parameters.
  // HandleEnvelope reads first the input and if everything is parsed fine, it sends
  // back another header, letter pair.
  virtual ::util::Status HandleEnvelope(uint64_t rpc_id, Envelope* input,
                                        EnvelopeWriter writer) = 0;
};

class ServiceInterface : public ListenerInterface {
 public:
  virtual ~ServiceInterface() {}

 protected:
  // A factory method creating a handler that should handles requests for a single connection.
  // The ownership over handler is passed to the caller.
  virtual ConnectionBridge* CreateConnectionBridge() = 0;

  ConnectionHandler* NewConnection() final;
};

}  // namespace rpc
}  // namespace util
