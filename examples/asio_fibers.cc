// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "base/init.h"
#include "base/logging.h"
#include "strings/strcat.h"

#include <experimental/optional>
#include <boost/asio.hpp>
#include <boost/optional.hpp>
#include <boost/fiber/condition_variable.hpp>
#include <boost/fiber/context.hpp>
#include <boost/fiber/mutex.hpp>
#include <boost/fiber/operations.hpp>  // for this_fiber.
#include <boost/fiber/scheduler.hpp>

#include "examples/yield.hpp"
#include "util/http/http_server.h"
#include "util/http/varz_stats.h"

using namespace boost;

DEFINE_int32(port, 8080, "Port number.");

using asio::ip::tcp;

typedef std::shared_ptr< tcp::socket > socket_ptr;
http::VarzQps qps("echo-qps");


typedef intrusive::list_member_hook<
    intrusive::link_mode<intrusive::auto_unlink>> connection_hook;

struct Connection {
  connection_hook hook_;
  fibers::fiber fiber;

  tcp::socket socket;
  fibers::condition_variable* on_exit;

  Connection(asio::io_service* io_svc, fibers::condition_variable* cv )
    : socket(*io_svc), on_exit(cv) {}

};

typedef intrusive::list<
                Connection,
                intrusive::member_hook<
                    Connection, connection_hook, &Connection::hook_>,
                intrusive::linear<true>, intrusive::cache_last< true >,
                intrusive::constant_time_size<false>
            > ConnectionList;


class round_robin : public fibers::algo::algorithm {
 private:
  std::shared_ptr< asio::io_service >      io_svc_;
  asio::steady_timer                       suspend_timer_;
//]
    fibers::scheduler::ready_queue_type      rqueue_;
    fibers::mutex                            mtx_;
    fibers::condition_variable               cnd_;
    std::size_t                                     counter_{ 0 };

 public:
  // [asio_rr_service_top
  struct service : public asio::io_service::service {
    static asio::io_service::id                  id;

    std::unique_ptr< asio::io_service::work >    work_;

    service(asio::io_service& io_svc) : asio::io_service::service(io_svc),
      work_{ new asio::io_service::work( io_svc) } {
    }

    virtual ~service() {}

    service( service const&) = delete;
    service & operator=( service const&) = delete;

    void shutdown_service() final {
        work_.reset();
    }
  };
//]

//[asio_rr_ctor
  round_robin( std::shared_ptr< asio::io_service > const& io_svc) :
      io_svc_( io_svc),
      suspend_timer_( * io_svc_) {
      // We use add_service() very deliberately. This will throw
      // service_already_exists if you pass the same io_service instance to
      // more than one round_robin instance.
      asio::add_service( * io_svc_, new service( * io_svc_) );

      asio::post(*io_svc_, [this] () mutable { this->loop();});
  }

  void awakened( fibers::context * ctx) noexcept {
      BOOST_ASSERT( nullptr != ctx);
      BOOST_ASSERT( ! ctx->ready_is_linked() );
      ctx->ready_link( rqueue_); /*< fiber, enqueue on ready queue >*/
      if ( ! ctx->is_context( fibers::type::dispatcher_context) ) {
          ++counter_;
      }
      // VLOG(1) << "awakened: " << ctx->get_id();
  }

    fibers::context * pick_next() noexcept {
      fibers::context * ctx( nullptr);
      if ( ! rqueue_.empty() ) { /*<
          pop an item from the ready queue
      >*/
          ctx = & rqueue_.front();
          rqueue_.pop_front();
          BOOST_ASSERT( nullptr != ctx);
          BOOST_ASSERT( fibers::context::active() != ctx);
          if ( ! ctx->is_context( fibers::type::dispatcher_context) ) {
              --counter_;
          }
      }
      return ctx;
    }

    bool has_ready_fibers() const noexcept {
        return 0 < counter_;
    }

    void suspend_until( std::chrono::steady_clock::time_point const& abs_time) noexcept final {
        // Set a timer so at least one handler will eventually fire, causing
        // run_one() to eventually return.
      if ( (std::chrono::steady_clock::time_point::max)() != abs_time) {
      // Each expires_at(time_point) call cancels any previous pending
      // call. We could inadvertently spin like this:
      // dispatcher calls suspend_until() with earliest wake time
      // suspend_until() sets suspend_timer_
      // lambda loop calls run_one()
      // some other asio handler runs before timer expires
      // run_one() returns to lambda loop
      // lambda loop yields to dispatcher
      // dispatcher finds no ready fibers
      // dispatcher calls suspend_until() with SAME wake time
      // suspend_until() sets suspend_timer_ to same time, canceling
      // previous async_wait()
      // lambda loop calls run_one()
      // asio calls suspend_timer_ handler with operation_aborted
      // run_one() returns to lambda loop... etc. etc.
      // So only actually set the timer when we're passed a DIFFERENT
      // abs_time value.
          suspend_timer_.expires_at( abs_time);
          suspend_timer_.async_wait([](system::error_code const&){
                                      this_fiber::yield();
                                    });
      }
      cnd_.notify_one();
    }
//]

  void notify() noexcept {
      // Something has happened that should wake one or more fibers BEFORE
      // suspend_timer_ expires. Reset the timer to cause it to fire
      // immediately, causing the run_one() call to return. In theory we
      // could use cancel() because we don't care whether suspend_timer_'s
      // handler is called with operation_aborted or success. However --
      // cancel() doesn't change the expiration time, and we use
      // suspend_timer_'s expiration time to decide whether it's already
      // set. If suspend_until() set some specific wake time, then notify()
      // canceled it, then suspend_until() was called again with the same
      // wake time, it would match suspend_timer_'s expiration time and we'd
      // refrain from setting the timer. So instead of simply calling
      // cancel(), reset the timer, which cancels the pending sleep AND sets
      // a new expiration time. This will cause us to spin the loop twice --
      // once for the operation_aborted handler, once for timer expiration
      // -- but that shouldn't be a big problem.
      suspend_timer_.async_wait([](boost::system::error_code const&){
                                  this_fiber::yield();
                                });
      suspend_timer_.expires_at( std::chrono::steady_clock::now() );
  }

//]

 private:
    void loop() {
      while ( ! io_svc_->stopped() ) {
        if ( has_ready_fibers() ) {
            // run all pending handlers in round_robin
            while ( io_svc_->poll() );
            // block this fiber till all pending (ready) fibers are processed
            // == round_robin::suspend_until() has been called
            std::unique_lock< fibers::mutex > lk( mtx_);
            cnd_.wait(lk);
        } else {
            // run one handler inside io_service
            // if no handler available, block this thread

            if ( ! io_svc_->run_one() ) {
                break;
            }
        }
      }
    }
};

asio::io_service::id round_robin::service::id;

constexpr unsigned max_length = 1024;

/*****************************************************************************
*   fiber function per server connection
*****************************************************************************/
// TODO: there is a bug here:
// if server stops, this socket still block and the fiber does not exit.
// There is resource leak and fiber scheduler will deadlock.
// Solution: we should have the notion of connection that tracks all its
// resources (fiber, socket etc)
// Server should access the list of all active connections and release them upon exit.
// In addition, each fiber should be able to remove its connection from the list when exiting.
// One way to do it is to use intrusive linked list. That way each connection ptr could remove itself
// and no allocations are needed. For example, intrusive::slist, like terminated_queue_type in scheduler.hpp.
// libs/asio/example/cpp03/http/server/server.hpp uses simpler way to handle the connections using
// regular containers.
void session(Connection* conn) {
    tcp::socket* sock = &conn->socket;
    try {
        char data[max_length];
        boost::system::error_code ec;

        for (;;) {
            std::size_t length = sock->async_read_some(asio::buffer(data),
                                                       fibers::asio::yield[ec]);

            LOG(INFO) << "After async_read_some";
            if ( ec == asio::error::eof) {
                LOG(INFO) << "Connection closed";
                break; //connection closed cleanly by peer
            } else if ( ec) {
              throw system::system_error( ec); //some other error
            }
            LOG(INFO) << ": handled: " << std::string(data, length);
            asio::async_write(*sock, asio::buffer(data, length),
                              fibers::asio::yield[ec]);
            if ( ec == asio::error::eof) {
                break; //connection closed cleanly by peer
            } else if ( ec) {
                throw system::system_error( ec); //some other error
            }
        }
        LOG(INFO) << ": connection closed";
    } catch ( std::exception const& ex) {
        LOG(WARNING) << ": caught exception : ", ex.what();
    }
    sock->close();
    conn->hook_.unlink();
    conn->on_exit->notify_one();
    delete conn;

    LOG(INFO) << "Session closed";
}


void server(const std::shared_ptr<asio::io_service>& io_svc, tcp::acceptor & a) {
    LOG(INFO) << ": echo-server started";
    ConnectionList clist;
    DCHECK(clist.empty());
    fibers::condition_variable empty_list;

    try {

        for (;;) {
            std::unique_ptr<Connection> conn(new Connection(io_svc.get(), &empty_list));
            system::error_code ec;
            a.async_accept(
                    conn->socket,
                    fibers::asio::yield[ec]);

            if ( ec) {
                throw system::system_error( ec); //some other error
            } else {
              clist.push_back(*conn);
              DCHECK(!clist.empty());

              DCHECK(conn->hook_.is_linked());
              fibers::fiber(session, conn.release()).detach();
            }
        }
    } catch (std::exception const& ex) {
      LOG(WARNING) << ": caught exception : " << ex.what();
    }

    LOG(INFO) << "Cleaning " << clist.size() << " connections";

    for (auto it = clist.begin(); it != clist.end(); ++it) {
      it->socket.close();
    }

    LOG(INFO) << "Waiting for connections to close";

    fibers::mutex mtx;
    std::unique_lock<fibers::mutex> lk(mtx);
    empty_list.wait(lk, [&clist] { return clist.empty(); });
    io_svc->stop();
    LOG(INFO) << ": echo-server stopped";
}


/*****************************************************************************
*   fiber function per client
*****************************************************************************/
void client( std::shared_ptr< boost::asio::io_service > const& io_svc, tcp::acceptor & a,
             boost::fibers::barrier& barrier, unsigned iterations) {
  LOG(INFO) << ": echo-client started";

  for (unsigned count = 0; count < iterations; ++count) {
    tcp::resolver resolver( * io_svc);
    tcp::resolver::query query( tcp::v4(), "127.0.0.1", "9999");
    tcp::resolver::iterator iterator = resolver.resolve( query);
    tcp::socket s( *io_svc);
    boost::asio::connect(s, iterator);
    char msgbuf[512];
    char reply[max_length];

    for (unsigned msg = 0; msg < 1; ++msg) {
        char* next = StrAppend(msgbuf, sizeof(msgbuf), {count, ".", msg});

        LOG(INFO) << ": Sending: " << msgbuf;

        boost::system::error_code ec;
        asio::async_write(
                s, asio::buffer(msgbuf, next - msgbuf),
                fibers::asio::yield[ec]);
        if ( ec == asio::error::eof) {
          return; //connection closed cleanly by peer
        } else if ( ec) {
          throw system::system_error( ec); //some other error
        }

        size_t reply_length = s.async_read_some(
                boost::asio::buffer(reply, max_length),
                boost::fibers::asio::yield[ec]);
        if ( ec == boost::asio::error::eof) {
            return; //connection closed cleanly by peer
        } else if ( ec) {
            throw boost::system::system_error( ec); //some other error
        }
        LOG(INFO) << ": Reply  : ", std::string( reply, reply_length);
      }
  }
  // done with all iterations, wait for rest of client fibers
  if ( barrier.wait()) {
    // exactly one barrier.wait() call returns true
    // we're the lucky one
    a.close();
    LOG(INFO) << ": acceptor stopped";
  }
  LOG(INFO) << ": echo-client stopped";
}

int main(int argc, char **argv) {
  MainInitGuard guard(&argc, &argv);

  http::Server http_server(FLAGS_port);
  util::Status status = http_server.Start();
  CHECK(status.ok()) << status;

  std::shared_ptr<asio::io_service> io_svc = std::make_shared<asio::io_service >();
  fibers::use_scheduling_algorithm<round_robin>(io_svc);

  asio::signal_set signals(*io_svc, SIGINT, SIGTERM);

  tcp::acceptor acc(*io_svc, tcp::endpoint( tcp::v4(), 9999) );

  signals.async_wait(
      [&](boost::system::error_code /*ec*/, int /*signo*/) {
        // The server is stopped by cancelling all outstanding asynchronous
        // operations. Once all operations have finished the io_context::run()
        // call will exit.
        acc.close();
        // io_svc->stop();
        LOG(INFO) << "Signals close";
      });


  fibers::fiber srv_fb(server, io_svc, std::ref(acc));
  //srv_fb.detach();

  LOG(INFO) << "Active context is " << fibers::context::active();
  io_svc->run();
  DCHECK(srv_fb.joinable());

  srv_fb.join();
  LOG(INFO) << "After join";
  return 0;
}
