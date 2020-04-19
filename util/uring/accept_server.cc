// Copyright 2020, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/uring/accept_server.h"

#include <boost/fiber/operations.hpp>

#include "base/logging.h"
#include "util/uring/fiber_socket.h"
#include "util/uring/proactor_pool.h"
#include "util/uring/uring_fiber_algo.h"

namespace util {
namespace uring {

using namespace boost;
using namespace std;

AcceptServer::AcceptServer(ProactorPool* pool, bool break_on_int)
    : pool_(pool), ref_bc_(0), break_(break_on_int) {
}

AcceptServer::~AcceptServer() {
}

void AcceptServer::Run() {
  if (!listen_wrapper_.empty()) {
    ref_bc_.Add(listen_wrapper_.size());

    for (auto& lw : listen_wrapper_) {
      lw.listener.proactor()->AsyncFiber(&AcceptServer::RunAcceptLoop, this, &lw);
    }
  }
  was_run_ = true;
}

// If wait is false - does not wait for the server to stop.
// Then you need to run Wait() to wait for proper shutdown.
void AcceptServer::Stop(bool wait) {
  VLOG(1) << "AcceptServer::Stop";

  BreakListeners();
  if (wait)
    Wait();
}

void AcceptServer::Wait() {
  VLOG(1) << "AcceptServer::Wait";
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
  CHECK(!ec) << "Could not open port " << port << " " << ec << "/" << ec.message();

  auto ep = fs.LocalEndpoint();
  lii->RegisterPool(pool_);

  Proactor* next = pool_->GetNextProactor();
  fs.set_proactor(next);

  listen_wrapper_.emplace_back(lii, std::move(fs));

  return ep.port();
}

void AcceptServer::BreakListeners() {
  for (auto& lw : listen_wrapper_) {
    lw.listener.proactor()->Async([sock = &lw.listener] { sock->Shutdown(SHUT_RDWR); });
  }
}

// Runs in a dedicated fiber for each listener.
void AcceptServer::RunAcceptLoop(ListenerWrapper* lw) {
  auto& fiber_props = this_fiber::properties<UringFiberProps>();
  fiber_props.set_name("AcceptLoop");

  auto ep = lw->listener.LocalEndpoint();
  LOG(INFO) << "AcceptServer - listening on port " << ep.port();

  using ListType =
      intrusive::slist<Connection, Connection::member_hook_t, intrusive::constant_time_size<false>,
                       intrusive::cache_last<false>>;

  // Holds all the connection references in the process. For each thread - its own list.
  thread_local ListType conn_list;
  fibers_ext::BlockingCounter conn_count{0};

  while (true) {
    FiberSocket peer;
    std::error_code ec = lw->listener.Accept(&peer);
    if (ec == errc::connection_aborted)
      break;

    if (ec) {
      LOG(ERROR) << "Error calling accept " << ec << "/" << ec.message();
      break;
    }
    VLOG(2) << "Accepted " << peer.native_handle() << ": " << peer.LocalEndpoint();
    Proactor* next = pool_->GetNextProactor();  // Could be for another thread.

    peer.set_proactor(next);

    // mutable because we move peer.
    auto cb = [&conn_count, peer = std::move(peer), next, li = lw->lii]() mutable {
      Connection* conn = li->NewConnection(next);
      conn_list.push_front(*conn);
      conn->SetSocket(std::move(peer));
      conn_count.Add(1);
      next->AsyncFiber(&RunSingleConnection, conn, &conn_count);
    };

    // Run cb in its Proactor thread.
    next->Await(std::move(cb));
  }

  lw->lii->PreShutdown();

  pool_->AsyncOnAll([&](Proactor*) {
    for (auto& val : conn_list) {
      val.socket_.Shutdown(SHUT_RDWR);
    }
  });

  VLOG(1) << "Waiting for connections to close";
  conn_count.Wait();

  lw->lii->PostShutdown();

  LOG(INFO) << "Accept server stopped for port " << ep.port();
  ref_bc_.Dec();
}

void AcceptServer::RunSingleConnection(Connection* conn, fibers_ext::BlockingCounter* bc) {
  std::unique_ptr<Connection> guard(conn);
  try {
    conn->HandleRequests();
  } catch (std::exception& e) {
    LOG(ERROR) << "Uncaught exception " << e.what();
  }
  bc->Dec();
}

void ListenerInterface::RegisterPool(ProactorPool* pool) {
  // In tests we might relaunch AcceptServer with the same listener, so we allow
  // reassigning the same pool.
  CHECK(pool_ == nullptr || pool_ == pool);

  pool_ = pool;
}

}  // namespace uring
}  // namespace util
