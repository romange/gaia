// Copyright 2020, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <functional>
#include <vector>

#include "util/fibers/fibers_ext.h"
#include "util/uring/connection.h"
#include "util/uring/fiber_socket.h"

namespace util {
namespace uring {

class Proactor;
class Connection;

/**
 * @brief Abstracts away connections implementation and their life-cycle.
 *
 */
class ListenerInterface {
 public:
  virtual ~ListenerInterface() {
  }

  void RegisterPool(Proactor* pool);

  // Creates a dedicated handler for a new connection.
  virtual Connection* NewConnection(Proactor* context) = 0;

  // Called by AcceptServer when shutting down start and before all connections are closed.
  virtual void PreShutdown() {
  }

  // Called by AcceptServer when shutting down finalized and after all connections are closed.
  virtual void PostShutdown() {
  }

 protected:
  Proactor* pool() {
    return pool_;
  }

 private:
  Proactor* pool_ = nullptr;
};

class AcceptServer {
 public:
  explicit AcceptServer(Proactor* pool, bool break_on_int = true);
  ~AcceptServer();

  void Run();

  // If wait is false - does not wait for the server to stop.
  // Then you need to run Wait() to wait for proper shutdown.
  void Stop(bool wait = false);

  void Wait();

  // Returns the port number to which the listener was bound.
  unsigned short AddListener(unsigned short port, ListenerInterface* cf);

  void TriggerOnBreakSignal(std::function<void()> f) {
    on_break_hook_ = std::move(f);
  }

  void set_back_log(uint16_t backlog) {
    backlog_ = backlog;
  }

 private:
  struct ListenerWrapper {
    Proactor* accept_proactor;
    ListenerInterface* lii;
    FiberSocket listener;

    ListenerWrapper(Proactor* aproactor, ListenerInterface* l, FiberSocket fs)
        : accept_proactor(aproactor), lii(l), listener(std::move(fs)) {
    }
    ListenerWrapper(ListenerWrapper&&) noexcept = default;
  };

  void RunAcceptLoop(ListenerWrapper* lw);
  static void RunSingleConnection(Proactor* p, Connection* conn);

  void BreakListeners();


  Proactor* pool_;

  // Called if a termination signal has been caught (SIGTERM/SIGINT).
  std::function<void()> on_break_hook_;

  std::vector<ListenerWrapper> listen_wrapper_;
  fibers_ext::BlockingCounter ref_bc_;  // to synchronize listener threads during the shutdown.

  bool was_run_ = false;
  uint16_t backlog_ = 128;
};

}  // namespace uring
}  // namespace util
