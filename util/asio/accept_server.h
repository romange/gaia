// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <tuple>

#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/signal_set.hpp>

#include "util/asio/connection_handler.h"
#include "util/fibers_ext.h"

namespace util {

class IoContextPool;

class AcceptServer {
 public:
  typedef ::boost::asio::io_context io_context;
  typedef ::boost::fibers::condition_variable condition_variable;

  using ConnectionFactory = std::function<ConnectionHandler*()> ;

  explicit AcceptServer(IoContextPool* pool);
  ~AcceptServer();

  void Run();

  void Stop() {
    // No need to close acceptor because signals.cancel will trigger its callback that
    // will close it anyway.
    signals_.cancel();
  }

  void Wait();

  // Returns the port number to which the listener was bound.
  unsigned short AddListener(unsigned short port, ConnectionFactory cf);

 private:
  using acceptor = ::boost::asio::ip::tcp::acceptor;
  using endpoint = ::boost::asio::ip::tcp::endpoint;
  struct Listener;

  void RunInIOThread(Listener* listener);

  // Should really be std::expected or std::experimental::fundamental_v3::expected
  // when upgrade the compiler.
  typedef std::tuple<ConnectionHandler*, ::boost::system::error_code>
    AcceptResult;

  AcceptResult AcceptConnection(Listener* listener, ConnectionHandler::Notifier* done);

  IoContextPool* pool_;

  struct Listener {
    ::boost::asio::ip::tcp::acceptor acceptor;
    ConnectionFactory cf;
    unsigned short port;

    Listener(io_context* cntx, const endpoint& ep,
             ConnectionFactory cf2) : acceptor(*cntx, ep), cf(std::move(cf2)) {
      port = acceptor.local_endpoint().port();
    }
  };

  ::boost::asio::signal_set signals_;
  fibers_ext::BlockingCounter bc_;

  bool was_run_ = false;

  std::vector<Listener> listeners_;
};

}  // namespace util
