// Copyright 2020, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/uring/accept_server.h"

#include <boost/fiber/operations.hpp>

#include "base/logging.h"
#include "util/uring/fiber_socket.h"
#include "util/uring/proactor.h"
#include "util/uring/uring_fiber_algo.h"

namespace util {
namespace uring {

using namespace boost;
using namespace std;

AcceptServer::AcceptServer(Proactor* pool, bool break_on_int) : pool_(pool), ref_bc_(0) {
  // TBD: to register signals in order to stop the loop gracefully.
}

AcceptServer::~AcceptServer() {
}

void AcceptServer::Run() {
  if (!listen_wrapper_.empty()) {
    ref_bc_.Add(listen_wrapper_.size());

    for (auto& lw : listen_wrapper_) {
      lw.accept_proactor->AsyncFiber(&AcceptServer::RunAcceptLoop, this, &lw);
    }
  }
  was_run_ = true;
}

// If wait is false - does not wait for the server to stop.
// Then you need to run Wait() to wait for proper shutdown.
void AcceptServer::Stop(bool wait) {
  BreakListeners();
  if (wait)
    Wait();
}

void AcceptServer::Wait() {
  if (was_run_) {
    ref_bc_.Wait();
    VLOG(1) << "AcceptServer::Wait completed";
  } else {
    CHECK(listen_wrapper_.empty()) << "Must Call AcceptServer::Run() after adding listeners";
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

  auto ep = fs.LocalEndpoint();
  lii->RegisterPool(pool_);

  Proactor* next = pool_;

  listen_wrapper_.emplace_back(next, lii, std::move(fs));

  return ep.port();
}

void AcceptServer::BreakListeners() {
  for (auto& lw : listen_wrapper_) {
    lw.accept_proactor->Async([sock = &lw.listener] {});
  }
}

void AcceptServer::RunAcceptLoop(ListenerWrapper* lw) {
  auto& fiber_props = this_fiber::properties<UringFiberProps>();
  fiber_props.set_name("AcceptLoop");

  auto ep = lw->listener.LocalEndpoint();
  LOG(INFO) << "AcceptServer - listening on port " << ep.port();

  while (true) {
    FiberSocket peer;
    std::error_code ec = lw->listener.Accept(lw->accept_proactor, &peer);
    if (ec == errc::connection_aborted)
      break;
    if (ec) {
      LOG(ERROR) << "Error calling accept " << ec << "/" << ec.message();
      break;
    }
    VLOG(2) << "Accepted " << peer.native_handle() << ": " << peer.LocalEndpoint();
    Proactor* next = pool_;

    Connection* conn = lw->lii->NewConnection(next);
  }
  lw->lii->PreShutdown();
  // TODO: to close all connections. We can do it by declaring thread_local intrusive list of
  // connections and go over it here via pool delegation.
  lw->lii->PostShutdown();

  LOG(INFO) << "Accept server stopped for port " << ep.port();
  ref_bc_.Dec();
}

void ListenerInterface::RegisterPool(Proactor* pool) {
  // In tests we might relaunch AcceptServer with the same listener, so we allow
  // reassigning the same pool.
  CHECK(pool_ == nullptr || pool_ == pool);

  pool_ = pool;
}

}  // namespace uring
}  // namespace util
