// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <absl/types/optional.h>

#include <boost/asio/ip/tcp.hpp>
#include <boost/intrusive/list.hpp>
#include <boost/intrusive/slist_hook.hpp>
#include <boost/smart_ptr/intrusive_ptr.hpp>

#include "util/asio/fiber_socket.h"

namespace util {

class IoContextPool;
class IoContext;
class AcceptServer;

namespace detail {
using namespace ::boost::intrusive;

// auto_unlink allows unlinking from the container during the destruction of
// of hook without holding the reference to the container itself.
// Requires that the container won't have O(1) size function.
typedef slist_member_hook<link_mode<auto_unlink>> connection_hook;
class Flusher;

}  // namespace detail

// An instance of this class handles a single connection in fiber.
class ConnectionHandler {
  friend class AcceptServer;
  friend class detail::Flusher;

  using connection_hook_t = detail::connection_hook;

  connection_hook_t hook_;
  connection_hook_t flush_hook_;

 public:
  using ptr_t = ::boost::intrusive_ptr<ConnectionHandler>;
  using io_context = ::boost::asio::io_context;

  using member_hook_t =
      detail::member_hook<ConnectionHandler, detail::connection_hook, &ConnectionHandler::hook_>;

  using flush_hook_t = detail::member_hook<ConnectionHandler, detail::connection_hook,
                                           &ConnectionHandler::flush_hook_>;

  explicit ConnectionHandler(IoContext* context) noexcept;

  virtual ~ConnectionHandler();

  void Init(::boost::asio::ip::tcp::socket&& sock);

  void Close();

  IoContext& context() {
    return io_context_;
  }

  friend void intrusive_ptr_add_ref(ConnectionHandler* ctx) noexcept {
    ctx->use_count_.fetch_add(1, std::memory_order_relaxed);
  }

  friend void intrusive_ptr_release(ConnectionHandler* ctx) noexcept {
    if (1 == ctx->use_count_.fetch_sub(1, std::memory_order_release)) {
      // We want to synchronize on all changes to ctx performed in other threads.
      // ctx is not atomic but we know that whatever was being written - has been written
      // in other threads and no references to ctx exist anymore.
      // Therefore acquiring fence is enough to synchronize.
      // "acquire" requires a release opearation to mark the end of the memory changes we wish
      // to acquire, and "fetch_sub(std::memory_order_release)" provides this marker.
      // To summarize: fetch_sub(release) and fence(acquire) needed to order and synchronize
      // on changes on ctx in most performant way.
      // See: https://stackoverflow.com/q/27751025/
      std::atomic_thread_fence(std::memory_order_acquire);
      delete ctx;
    }
  }

 protected:
  //! Called to flush pending writes to the socket. Returns true if flush took place,
  // false overthise.
  virtual bool FlushWrites() { return false; }

  //! Called once after connection was initialized. Will run in io context thread of this handler.
  virtual void OnOpenSocket() {
  }

  /**
   * @brief Called before ConnectionHandler destroyed but after the socket was signalled
   * to stop and shutdown
   *
   * The function implementation may block the calling fiber.
   * Derived ConnectionHandler should clean here resources that must closed
   * before the object is destroyed. Will run in io context thread of the socket.
   */
  virtual void OnCloseSocket() {
  }

  // Should not block the thread. Can fiber-block (fiber friendly).
  virtual boost::system::error_code HandleRequest() = 0;

  absl::optional<FiberSyncSocket> socket_;

  IoContext& io_context_;

  //! If set to true, the frameworks will setup a flusher fiber that will call FlushWrites() method
  //! few times per msec.
  bool use_flusher_fiber_ = false;

 private:
  void RunInIOThread();

  std::atomic<std::uint32_t> use_count_{0};
};

/**
 * @brief Abstracts away connections implementation and their life-cycle.
 *
 */
class ListenerInterface {
 public:
  virtual ~ListenerInterface() {
  }

  void RegisterPool(IoContextPool* pool);

  // Creates a dedicated handler for a new connection.
  virtual ConnectionHandler* NewConnection(IoContext& context) = 0;

  // Called by AcceptServer when shutting down start and before all connections are closed.
  virtual void PreShutdown() {
  }

  // Called by AcceptServer when shutting down finalized and after all connections are closed.
  virtual void PostShutdown() {
  }

 protected:
  IoContextPool* pool() {
    return pool_;
  }

 private:
  IoContextPool* pool_ = nullptr;
};

}  // namespace util
