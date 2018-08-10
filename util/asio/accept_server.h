// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <tuple>

#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/signal_set.hpp>

#include "util/asio/connection.h"
#include "util/fibers_done.h"

namespace util {

class IoContextPool;

class AcceptServer {
 public:
  typedef ::boost::asio::io_context io_context;
  typedef ::boost::fibers::condition_variable condition_variable;

  typedef std::function<ConnectionHandler*(io_context* cntx,
                                           condition_variable* done)> ConnectionFactory;

  AcceptServer(unsigned short port, IoContextPool* pool, ConnectionFactory cf);

  void Run();
  void Wait();

 private:
  void RunInIOThread();

  // Should really be std::expected or std::experimental::fundamental_v3::expected
  // when upgrade the compiler.
  typedef std::tuple<ConnectionHandler*, ::boost::system::error_code>
    AcceptResult;

  AcceptResult AcceptFiber(condition_variable* done);

  IoContextPool* pool_;
  io_context& io_cntx_;
  ::boost::asio::ip::tcp::acceptor acceptor_;
  ::boost::asio::signal_set signals_;
  fibers_ext::Done done_;
  ConnectionFactory cf_;
};

}  // namespace util
