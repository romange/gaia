// Copyright 2020, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/uring/accept_server.h"

#include "base/logging.h"

#include "util/uring/fiber_socket.h"
#include "util/uring/proactor.h"

namespace util {
namespace uring {

AcceptServer::AcceptServer(Proactor* pool) : pool_(pool), ref_bc_(0) {
}

AcceptServer::~AcceptServer() {
}

void AcceptServer::Run() {
  if (!listeners_.empty()) {
    ref_bc_.Add(listeners_.size());

    for (auto& listener : listeners_) {
      ListenerWrapper* ptr = &listener;
      ptr->accept_proactor->AsyncFiber(&AcceptServer::RunAcceptLoop, this, ptr);
    }
  }
  was_run_ = true;
}

// If wait is false - does not wait for the server to stop.
// Then you need to run Wait() to wait for proper shutdown.
void AcceptServer::Stop(bool wait) {
}

void AcceptServer::Wait() {
  if (was_run_) {
    ref_bc_.Wait();
    VLOG(1) << "AcceptServer::Wait completed";
  } else {
    CHECK(listeners_.empty()) << "Must Call AcceptServer::Run() after adding listeners";
  }
}

// Returns the port number to which the listener was bound.
unsigned short AcceptServer::AddListener(unsigned short port, ListenerInterface* lii) {
  CHECK(lii);

  // We can not allow dynamic listener additions because listeners_ might reallocate.
  CHECK(!was_run_);

  FiberSocket fs;
  auto ec = fs.Listen(port, backlog_);
  CHECK(ec) << "Could not open port " << port << " " << ec << "/" << ec.message();

  lii->RegisterPool(pool_);

  auto ep = fs.LocalEndpoint();
  LOG(INFO) << "AcceptServer - listening on port " << ep.port();

  Proactor* next = pool_;

  listeners_.emplace_back(next, lii, std::move(fs));

  return ep.port();
}

}  // namespace uring
}  // namespace util
