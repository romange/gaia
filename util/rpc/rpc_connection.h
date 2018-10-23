// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <memory>

#include "util/asio/accept_server.h"
#include "util/rpc/rpc_envelope.h"
#include "strings/stringpiece.h"

namespace util {

// RPC-Server side part.

class AcceptServer;

namespace rpc {

// Also defined in frame_format.h. Seems to work.
typedef uint64_t RpcId;

// ConnectionBridge is responsible to abstract higher level server-app logic and to provide
// an interface that allows to map Envelope to ServiceInterface methods.
// ConnectionBridge is a single-fiber creature, so currently only one caller fiber can
// use it simultaneusly.
class ConnectionBridge {
 public:
  typedef std::function<void(Envelope&&)> EnvelopeWriter;

  virtual ~ConnectionBridge() {}

  // header and letter are input/output parameters.
  // HandleEnvelope reads first the input and if everything is parsed fine, it sends
  // back another header, letter pair.
  virtual void HandleEnvelope(RpcId rpc_id, Envelope* input,
                              EnvelopeWriter writer) = 0;

  // In case HandleEnvelope is asynchronous, waits for all the issued calls to finish.
  // HandleEnvelope should not be called after calling Join().
  virtual void Join() {};
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
