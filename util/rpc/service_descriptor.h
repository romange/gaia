// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include <functional>

#include <google/protobuf/message.h>

#include "absl/strings/string_view.h"
#include "util/status.h"

namespace util {

// RPC-Server side part.

namespace rpc {

class ServiceDescriptor {
 public:
  struct MethodOptions {
    // Must be power of 2. If not - will be quietly rounded up to power of 2.
    uint32_t async_level = 0;
  };

  using Message = ::google::protobuf::Message;
  using StreamItemWriter = std::function<void(const Message*)>;
  using RpcMethodCb = std::function<util::Status(const Message&, Message* msg) noexcept>;
  using RpcStreamMethodCb = std::function<util::Status(const Message&, StreamItemWriter) noexcept>;

  ServiceDescriptor();

  virtual ~ServiceDescriptor();

  virtual size_t GetMethodByHash(absl::string_view method) const = 0;

  size_t size() const {
    return methods_.size();
  }

  void SetOptions(size_t index, const MethodOptions& opts);

  // Must be in header file due to size() accessor.
  // Must be public because RpcBridge needs to access it.
  // Static descriptor helping rpc framework to reroute generic on-wire envelopes to
  // specific rpc methods.
  struct Method {
    ::std::string name;
    MethodOptions options;

    // Only one of those defined.
    RpcMethodCb single_rpc_method;
    RpcStreamMethodCb stream_rpc_method;

    // Factory req/resp creators.
    const Message* default_req = nullptr;
    const Message* default_resp = nullptr;

    // Simple RPC
    Method(std::string n, RpcMethodCb c, const Message& dreq, const Message& dresp)
        : name(std::move(n)),
          single_rpc_method(std::move(c)),
          default_req(&dreq),
          default_resp(&dresp) {
    }

    // Streaming RPC.
    Method(std::string n, RpcStreamMethodCb c, const Message& dreq)
        : name(std::move(n)), stream_rpc_method(std::move(c)), default_req(&dreq) {
    }
  };

  const Method& method(size_t i) const {
    return methods_[i];
  }

 protected:
  std::vector<Method> methods_;  // Generated classes derive from this class and fill this field.
};

}  // namespace rpc
}  // namespace util
