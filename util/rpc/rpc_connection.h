// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <memory>

#include "util/asio/connection_handler.h"

#include "util/rpc/rpc_envelope.h"
#include "strings/stringpiece.h"

namespace util {

// RPC-Server side part.

namespace rpc {

// Also defined in frame_format.h. Seems to work.
typedef uint64_t RpcId;

// ConnectionBridge is responsible to abstract higher level server-app logic and to provide
// an interface that allows to map Envelope to ServiceInterface methods.
// ConnectionBridge is a single-fiber creature, so currently only one caller fiber can
// use it simultaneusly.
// ConnectionBridge can be asynchronous, i.e. it's main calling function HandleEnvelope
// can exit before it finishes writing to EnvelopeWriter.
class ConnectionBridge {
 public:
  typedef std::function<void(Envelope&&)> EnvelopeWriter;

  virtual ~ConnectionBridge() {}

  // Is called once from the connection thread before HandleEnvelope is being called.
  // Is intended to finalize the setup for the bridge inside its intended thread.
  virtual void InitInThread() {}

  // Main entry function that handles the input envelope and is responsible for
  // writing the results via writer.
  // HandleEnvelope first reads the input and if everything is parsed fine, it writes
  // back one or more envelopes via the writer. Specifics of the protocol are defined
  // in the derived class. Since HandleEnvelope can be asynchronous,
  // the caller should make sure the writer is valid through the call.
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
  // A factory method creating a handler that handles requests for a single connection.
  // The ownership over handler is passed to the caller.
  virtual ConnectionBridge* CreateConnectionBridge() = 0;

  ConnectionHandler* NewConnection(IoContext& context) final;
};

}  // namespace rpc
}  // namespace util
