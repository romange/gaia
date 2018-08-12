// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <memory>

#include <google/protobuf/message.h>

#include "util/asio/accept_server.h"
#include "util/status.h"

namespace util {

class AcceptServer;
class IoContextPool;

class RpcServiceDescriptor {
 public:
  using Message = ::google::protobuf::Message;

  struct MethodInfo {
    std::string name;
    Message* request;
    Message* response;

    std::function<Status(Message*, Message*)> client_method;

    MethodInfo() {}
    MethodInfo(std::string n, Message* req, Message* resp,
               std::function<Status(Message*, Message*)> c)
        : name(std::move(n)), request(req), response(resp), client_method(c) {}
  };

  const std::vector<MethodInfo>& methods() { return methods_; }

protected:
  virtual MethodInfo* GetMethodByHash(uint32_t hash) = 0;

  std::vector<MethodInfo> methods_;
};

class RpcServiceInterface {
 public:
  virtual ~RpcServiceInterface() {};

  // The ownership is passed to the caller.
  virtual RpcServiceDescriptor* GetDescriptor() const;
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
  std::unique_ptr<RpcServiceDescriptor> service_descr_;
};

}  // namespace util
