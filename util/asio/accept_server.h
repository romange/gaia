// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <tuple>

#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/signal_set.hpp>

#include "util/asio/connection_handler.h"
#include "util/fibers_done.h"

namespace util {

class IoContextPool;

class AcceptServer {
 public:
  typedef ::boost::asio::io_context io_context;
  typedef ::boost::fibers::condition_variable condition_variable;

  typedef std::function<ConnectionHandler*(io_context* cntx,
                                           ConnectionServerNotifier* notifier)> ConnectionFactory;

  AcceptServer(unsigned short port, IoContextPool* pool, ConnectionFactory cf);
  ~AcceptServer();

  void Run();

  void Stop() {
    // No need to close acceptor because signals.cancel will trigger its callback that
    // will close it anyway.
    signals_.cancel();
  }

  void Wait();

  unsigned short port() const { return port_;}
 private:
  void RunInIOThread();

  // Should really be std::expected or std::experimental::fundamental_v3::expected
  // when upgrade the compiler.
  typedef std::tuple<ConnectionHandler*, ::boost::system::error_code>
    AcceptResult;

  AcceptResult AcceptFiber(ConnectionServerNotifier* done);

  IoContextPool* pool_;
  io_context& io_cntx_;
  ::boost::asio::ip::tcp::acceptor acceptor_;
  ::boost::asio::signal_set signals_;
  fibers_ext::Done done_;
  ConnectionFactory cf_;
  bool was_run_ = false;
  unsigned short port_;
};

}  // namespace util
