// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <boost/asio/ip/tcp.hpp>
#include <boost/fiber/condition_variable.hpp>
#include <boost/intrusive/list.hpp>

namespace util {

using namespace boost::asio;
namespace intr = ::boost::intrusive;

class Connection {
  ip::tcp::socket socket_;
  boost::fibers::condition_variable& on_exit_;

 public:
  // auto_unlink allows unlinking from the container during the destruction of
  // of hook without holding the reference to the container itself.
  // Requires that the container won't have O(1) size function.
  typedef intr::list_member_hook<
      intr::link_mode<intr::auto_unlink>> connection_hook;

  connection_hook hook_;

  Connection(io_context* io_svc, boost::fibers::condition_variable* cv);

  void Run();

  ip::tcp::socket& socket() { return socket_;}

  typedef intr::member_hook<Connection, connection_hook, &Connection::hook_> member_hook_t;

 private:
  void Session();
  boost::system::error_code HandleOne();
};

// TODO: Do we need list or slist?
typedef intr::list<
                Connection,
                Connection::member_hook_t,
                intr::constant_time_size<false>> ConnectionList;


}  // namespace util
