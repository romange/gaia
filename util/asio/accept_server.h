// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <tuple>

#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/signal_set.hpp>

#include "util/asio/connection_handler.h"
#include "util/fibers/fibers_ext.h"

namespace util {

class IoContextPool;
class IoContext;

class AcceptServer {
 public:
  typedef ::boost::asio::io_context io_context;

  explicit AcceptServer(IoContextPool* pool);
  ~AcceptServer();

  void Run();

  // Does not wait for the server to stop.
  // You need to run Wait() to wait for proper shutdown.
  void Stop(bool wait = false) {
    // No need to close acceptor because signals.cancel will trigger its callback that
    // will close it anyway.
    signals_.cancel();
    if (wait)
      Wait();
  }

  void Wait();

  // Returns the port number to which the listener was bound.
  unsigned short AddListener(unsigned short port, ListenerInterface* cf);

  void TriggerOnBreakSignal(std::function<void()> f) { on_break_hook_ = std::move(f); }

 private:
  using acceptor = ::boost::asio::ip::tcp::acceptor;
  using endpoint = ::boost::asio::ip::tcp::endpoint;
  struct ListenerWrapper;

  void RunInIOThread(ListenerWrapper* listener);

  // Should really be std::expected or std::experimental::fundamental_v3::expected
  // when upgrade the compiler.
  typedef std::tuple<ConnectionHandler*, ::boost::system::error_code>
    AcceptResult;

  AcceptResult AcceptConnection(ListenerWrapper* listener);

  IoContextPool* pool_;

  struct ListenerWrapper {
    IoContext& io_context;
    ::boost::asio::ip::tcp::acceptor acceptor;
    ListenerInterface* listener;
    unsigned short port;

    ListenerWrapper(const endpoint& ep, IoContext* io_context,
                    ListenerInterface* si);
  };

  ::boost::asio::signal_set signals_;
  fibers_ext::BlockingCounter ref_bc_;
  std::vector<ListenerWrapper> listeners_;

  // Called if a termination signal has been caught (SIGTERM/SIGINT).
  std::function<void()> on_break_hook_;
  bool was_run_ = false;
};

}  // namespace util
