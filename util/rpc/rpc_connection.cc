// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "util/rpc/rpc_connection.h"

#include "base/logging.h"

#include "util/rpc/impl/rpc_conn_handler.h"

namespace util {
namespace rpc {

ConnectionHandler* ServiceInterface::NewConnection(IoContext& context) {
  ConnectionBridge* bridge = CreateConnectionBridge();
  return new RpcConnectionHandler(context, bridge);
}

}  // namespace rpc
}  // namespace util
