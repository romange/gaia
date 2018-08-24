// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <boost/asio/ip/tcp.hpp>
#include <boost/fiber/condition_variable.hpp>
#include <boost/intrusive/list.hpp>

namespace util {

namespace detail {
namespace intr = ::boost::intrusive;

// auto_unlink allows unlinking from the container during the destruction of
// of hook without holding the reference to the container itself.
// Requires that the container won't have O(1) size function.
typedef intr::list_member_hook<
        intr::link_mode<intr::auto_unlink>> connection_hook;
}  // namespace detail


class ConnectionServerNotifier {
  boost::fibers::condition_variable cnd_;
  boost::fibers::mutex mu_;

 public:
  explicit ConnectionServerNotifier() {}

  void Unlink(detail::connection_hook* hook);

  std::unique_lock<boost::fibers::mutex> Lock() {
    return std::unique_lock<boost::fibers::mutex>(mu_);
  }

  template<typename Predicate >
  void Wait(std::unique_lock<boost::fibers::mutex>& lock, Predicate pred) {
    cnd_.wait(lock, pred);
  }
};

// An instance of this class handles a single connection in fiber.
class ConnectionHandler {
 public:
  using io_context = ::boost::asio::io_context;
  using socket_t = ::boost::asio::ip::tcp::socket;

  detail::connection_hook hook_;

  ConnectionHandler(io_context* io_svc, ConnectionServerNotifier* channel);

  virtual ~ConnectionHandler();

  // Can be trigerred from any thread. Schedules RunInIOThread to run in io_context loop.
  void Run();

  void Close();

  socket_t& socket() { return socket_;}

  typedef detail::intr::member_hook<ConnectionHandler, detail::connection_hook,
                                    &ConnectionHandler::hook_> member_hook_t;

 protected:
  // Should not block the thread. Can fiber-block (fiber friendly).
  virtual boost::system::error_code HandleRequest() = 0;

  socket_t socket_;
  ConnectionServerNotifier* notifier_;
 private:
  void RunInIOThread();
};

// TODO: Do we need list or slist?
typedef detail::intr::list<
                ConnectionHandler,
                ConnectionHandler::member_hook_t,
                detail::intr::constant_time_size<false>> ConnectionHandlerList;


}  // namespace util
