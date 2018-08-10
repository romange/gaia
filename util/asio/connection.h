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

class ConnectionHandler {
 public:
  // auto_unlink allows unlinking from the container during the destruction of
  // of hook without holding the reference to the container itself.
  // Requires that the container won't have O(1) size function.
  typedef intr::list_member_hook<
      intr::link_mode<intr::auto_unlink>> connection_hook;

  connection_hook hook_;

  ConnectionHandler(io_context* io_svc, boost::fibers::condition_variable* done);

  virtual ~ConnectionHandler();

  // Can be trigerred from any thread. Schedules RunInIOThread to run in io_context loop.
  void Run();

  ip::tcp::socket& socket() { return socket_;}

  typedef intr::member_hook<ConnectionHandler, connection_hook, &ConnectionHandler::hook_>
    member_hook_t;

 protected:
  // Should not block the thread. Can fiber-block (fiber friendly).
  virtual boost::system::error_code HandleRequest() = 0;

  ip::tcp::socket socket_;
  boost::fibers::condition_variable& on_exit_;
 private:
  void RunInIOThread();
};

// TODO: Do we need list or slist?
typedef intr::list<
                ConnectionHandler,
                ConnectionHandler::member_hook_t,
                intr::constant_time_size<false>> ConnectionHandlerList;


}  // namespace util
