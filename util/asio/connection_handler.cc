// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/asio/connection_handler.h"

#include <boost/asio/dispatch.hpp>
#include <boost/asio/write.hpp>

#include "base/logging.h"
#include "util/asio/io_context.h"
#include "util/fibers/event_count.h"
#include "util/stats/varz_stats.h"

using namespace boost;
using namespace boost::asio;

using namespace std;

DEFINE_VARZ(VarzCount, connections);

namespace util {

namespace detail {

using FlushList = detail::slist<ConnectionHandler, ConnectionHandler::flush_hook_t,
                                detail::constant_time_size<false>, detail::cache_last<false>>;

class Flusher : public IoContext::Cancellable {
 public:
  Flusher() {
  }

  void Run() final;
  void Cancel() final;

  void Bind(ConnectionHandler* me) {
    flush_conn_list_.push_front(*me);
  }

  void Remove(ConnectionHandler* me) {
    // To make sure me does not run. Since Flusher and onnectionHandler fibers are
    // on the same thread, then either:
    // 1. Flusher is not running FlushWrites()- then we can just remove ourselves.
    //    and FlushList::iterator will be still valid where it points to.
    // 2. We are inside me->FlushWrites(). In that case mu_ is locked we want to wait till
    //    FlushWrites finishes before we remove outselves.
    std::unique_lock<fibers::mutex> lock(mu_);
    flush_conn_list_.erase(FlushList::s_iterator_to(*me));
  }

  void WakeIfNeeded() {
    if (sleep_) {
      sleep_ = false;
      sleep_ec_.notify();
    }
  }

 private:
  void SpinFlush();
  void Sleep();

  bool stop_ = false;
  bool sleep_ = false;  // we are thread-local so no need to atomic, thread-safety.

  fibers::condition_variable cv_;
  fibers::mutex mu_;

  //! We really use EventCount just to suspend/resume fibers unconditionally. Maybe it's cleaner
  //  just expose those methods directly.
  fibers_ext::EventCount sleep_ec_;
  FlushList flush_conn_list_;
};

void Flusher::Run() {
  VLOG(1) << "Flusher start ";

  while (!stop_) {
    SpinFlush();

    if (!stop_) {
      Sleep();
    }
  }
}

void Flusher::SpinFlush() {
  std::unique_lock<fibers::mutex> lock(mu_);

  uint32_t no_flush = 0;

  // We count iterations with no flushes at all.
  // If server does not need flushing for some time we return.
  while (no_flush < 100) {
    cv_.wait_for(lock, 300us);

    if (stop_)
      break;
    ++no_flush;
    for (auto it = flush_conn_list_.begin(); it != flush_conn_list_.end(); ++it) {
      if (it->FlushWrites()) {
        no_flush = 0;
      }
    }
  }
}

void Flusher::Sleep() {
  sleep_ = true;
  sleep_ec_.await([this] { return !sleep_;});
}

void Flusher::Cancel() {
  VLOG(1) << "Flusher::Cancel";
  stop_ = true;

  WakeIfNeeded();
  std::lock_guard<fibers::mutex> lock(mu_);
  cv_.notify_all();
}

}  // namespace detail

namespace {

inline bool IsExpectedFinish(system::error_code ec) {
  return ec == error::eof || ec == error::operation_aborted || ec == error::connection_reset ||
         ec == error::not_connected;
}

static thread_local detail::Flusher* local_flusher = nullptr;

}  // namespace

ConnectionHandler::ConnectionHandler(IoContext* context) noexcept : io_context_(*context) {
  CHECK_NOTNULL(context);
}

ConnectionHandler::~ConnectionHandler() {
}

void ConnectionHandler::Init(asio::ip::tcp::socket&& sock) {
  CHECK(!socket_ && sock.is_open());
  ip::tcp::no_delay nd(true);
  system::error_code ec;
  sock.set_option(nd, ec);
  if (ec)
    LOG(ERROR) << "Could not set socket option " << ec.message() << " " << ec;

  sock.non_blocking(true, ec);
  if (ec)
    LOG(ERROR) << "Could not make socket nonblocking " << ec.message() << " " << ec;

  socket_.emplace(std::move(sock));
  CHECK(socket_->is_open());
}

/*****************************************************************************
 *   fiber function per server connection
 *****************************************************************************/
void ConnectionHandler::RunInIOThread() {
  DCHECK(io_context_.InContextThread());

  connections.Inc();

  CHECK(socket_);
  OnOpenSocket();

  if (use_flusher_fiber_) {
    if (!local_flusher) {
      local_flusher = new detail::Flusher;
      io_context_.AttachCancellable(local_flusher);
    }
    local_flusher->Bind(this);
  }

  VLOG(1) << "ConnectionHandler::RunInIOThread: " << socket_->native_handle();
  system::error_code ec;

  try {
    while (socket_->is_open()) {
      ec = HandleRequest();
      if (UNLIKELY(ec)) {
        if (!IsExpectedFinish(ec)) {
          LOG(WARNING) << "[" << socket_->native_handle() << "] Error : " << ec.message() << ", "
                       << ec.category().name() << "/" << ec.value();
        }
        break;
      }
      if (use_flusher_fiber_) {
        local_flusher->WakeIfNeeded();
      }
    }
    VLOG(1) << "ConnectionHandler closed: " << socket_->native_handle();
  } catch (std::exception const& ex) {
    string str = ex.what();
    LOG(ERROR) << str;
  }

  if (use_flusher_fiber_) {
    local_flusher->Remove(this);
  }

  Close();

  connections.IncBy(-1);

  // RunInIOThread runs as lambda packaged with ptr_t guard on this. Once the lambda finishes,
  // it releases the ownership over this.
}

void ConnectionHandler::Close() {
  // Run Listener hook in the connection thread.
  io_context_.AwaitSafe([this] {
    if (!socket_->is_open())
      return;

    system::error_code ec;
    VLOG(1) << "Before shutdown " << socket_->native_handle();
    socket_->Shutdown(ec);
    VLOG(1) << "After shutdown: " << ec << " " << ec.message();

    // socket::close() closes the underlying socket and cancels the pending operations.
    // HOWEVER the problem is that those operations return with ec = ok()
    // so the flow  is not aware that the socket is closed.
    // That can lead to nasty bugs. Therefore the only place we close
    // socket is from the listener loop. Here we only signal that we are ready to close.
    OnCloseSocket();
  });
}

void ListenerInterface::RegisterPool(IoContextPool* pool) {
  // In tests we might relaunch AcceptServer with the same listener, so we allow
  // reassigning the same pool.
  CHECK(pool_ == nullptr || pool_ == pool);
  pool_ = pool;
}

}  // namespace util
