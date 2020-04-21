// Copyright 2020, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/uring/accept_server.h"

#include <boost/fiber/operations.hpp>

#include "base/logging.h"
#include "util/uring/fiber_socket.h"
#include "util/uring/proactor_pool.h"
#include "util/uring/uring_fiber_algo.h"

#define VSOCK(verbosity, sock) VLOG(verbosity) << "sock[" << (sock).native_handle() << "] "
#define DVSOCK(verbosity, sock) DVLOG(verbosity) << "sock[" << (sock).native_handle() << "] "

namespace util {
namespace uring {

using namespace boost;
using namespace std;

using ListType =
      intrusive::slist<Connection, Connection::member_hook_t, intrusive::constant_time_size<true>,
                       intrusive::cache_last<false>>;

struct AcceptServer::SafeConnList {
  ListType list;
  fibers::mutex mu;
  fibers::condition_variable cond;

  void Link(Connection* c) {
    std::lock_guard<fibers::mutex> lk(mu);
    list.push_front(*c);
    VLOG(2) << "List size " << list.size();
  }

  void Unlink(Connection* c) {
    std::lock_guard<fibers::mutex> lk(mu);
    auto it = list.iterator_to(*c);
    list.erase(it);
    DVLOG(2) << "List size " << list.size();

    if (list.empty()) {
      cond.notify_one();
    }
  }

  void AwaitEmpty() {
    std::unique_lock<fibers::mutex> lk(mu);
    DVLOG(1) << "AwaitEmpty: List size: " << list.size();

    cond.wait(lk, [this] { return list.empty(); });
  }
};

AcceptServer::AcceptServer(ProactorPool* pool, bool break_on_int)
    : pool_(pool), ref_bc_(0), break_(break_on_int) {
  if (break_on_int) {
    Proactor* proactor = pool_->GetNextProactor();
    proactor->RegisterSignal({SIGINT, SIGTERM}, [this](int signal) {
      LOG(INFO) << "Exiting on signal " << signal;
      BreakListeners();
    });
  }
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
    lw.listener.proactor()->AsyncBrief([sock = &lw.listener] { sock->Shutdown(SHUT_RDWR); });
  }
  VLOG(1) << "AcceptServer::BreakListeners finished";
}

// Runs in a dedicated fiber for each listener.
void AcceptServer::RunAcceptLoop(ListenerWrapper* lw) {
  auto& fiber_props = this_fiber::properties<UringFiberProps>();
  fiber_props.set_name("AcceptLoop");

  auto ep = lw->listener.LocalEndpoint();
  VSOCK(0, lw->listener) << "AcceptServer - listening on port " << ep.port();
  SafeConnList safe_list;

  lw->lii->PreAcceptLoop(lw->listener.proactor());

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
    Connection* conn = lw->lii->NewConnection(next);
    conn->SetSocket(std::move(peer));
    safe_list.Link(conn);

    // mutable because we move peer.
    auto cb = [conn, next, &safe_list]() mutable {
      next->AsyncFiber(&RunSingleConnection, conn, &safe_list);
    };

    // Run cb in its Proactor thread.
    next->AsyncFiber(std::move(cb));
  }

  lw->lii->PreShutdown();

  safe_list.mu.lock();
  unsigned cnt = 0;
  for (auto& val : safe_list.list) {
    val.socket_.Shutdown(SHUT_RDWR);
    DVSOCK(1, val.socket_) << "Shutdown";
    ++cnt;
  }

  safe_list.mu.unlock();

  VLOG(1) << "Waiting for " << cnt <<" connections to close";
  safe_list.AwaitEmpty();

  lw->lii->PostShutdown();

  LOG(INFO) << "Accept server stopped for port " << ep.port();
  ref_bc_.Dec();
}

void AcceptServer::RunSingleConnection(Connection* conn, SafeConnList* conns) {
  VSOCK(2, *conn) << "Running connection";

  std::unique_ptr<Connection> guard(conn);
  try {
    conn->HandleRequests();
    VSOCK(2, *conn) << "After HandleRequests";

  } catch (std::exception& e) {
    LOG(ERROR) << "Uncaught exception " << e.what();
  }
  conns->Unlink(conn);
}

void ListenerInterface::RegisterPool(ProactorPool* pool) {
  // In tests we might relaunch AcceptServer with the same listener, so we allow
  // reassigning the same pool.
  CHECK(pool_ == nullptr || pool_ == pool);

  pool_ = pool;
}

}  // namespace uring
}  // namespace util
