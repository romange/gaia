// Copyright 2020, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "base/init.h"
#include "examples/pingserver/ping_command.h"
#include "util/asio/accept_server.h"
#include "util/asio/io_context_pool.h"
#include "util/http/http_conn_handler.h"
#include "util/stats/varz_stats.h"
#include "util/uring/proactor.h"
#include "util/uring/uring_fiber_algo.h"

using namespace boost;
using namespace util;
using namespace uring;
using IoResult = Proactor::IoResult;

DEFINE_int32(http_port, 8080, "Http port.");
DEFINE_int32(port, 6380, "Redis port");
DEFINE_uint32(queue_depth, 256, "");
DEFINE_bool(linked_sqe, false, "If true, then no-op events are linked to the next ones");

VarzQps ping_qps("ping-qps");

static int SetupListenSock(int port) {
  struct sockaddr_in server_addr;
  int fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
  CHECK_GT(fd, 0);
  const int val = 1;
  setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val));

  memset(&server_addr, 0, sizeof(server_addr));
  server_addr.sin_family = AF_INET;
  server_addr.sin_port = htons(FLAGS_port);
  server_addr.sin_addr.s_addr = INADDR_ANY;

  constexpr uint32_t BACKLOG = 128;

  CHECK_EQ(0, bind(fd, (struct sockaddr*)&server_addr, sizeof(server_addr)))
      << "Error: " << strerror(errno);
  CHECK_EQ(0, listen(fd, BACKLOG));

  return fd;
}

class RedisConnection : public std::enable_shared_from_this<RedisConnection> {
 public:
  RedisConnection() {
    for (unsigned i = 0; i < 2; ++i) {
      memset(msg_hdr_ + i, 0, sizeof(msghdr));
      msg_hdr_[i].msg_iovlen = 1;
    }
    msg_hdr_[0].msg_iov = &io_rvec_;
    msg_hdr_[1].msg_iov = &io_wvec_;
  }

  void Handle(IoResult res, int32_t payload, Proactor* mgr);

  void StartPolling(int fd, Proactor* mgr);

 private:
  SubmitEntry GetEntry(int32_t socket, Proactor* mgr) {
    auto ptr = shared_from_this();
    auto cb = [ptr](IoResult res, int32_t payload, Proactor* proactor) {
      ptr->Handle(res, payload, proactor);
    };
    return mgr->GetSubmitEntry(std::move(cb), socket);
  }

  enum State { WAIT_READ, READ, WRITE } state_ = WAIT_READ;
  void InitiateRead(Proactor* mgr);
  void InitiateWrite(Proactor* mgr);

  int fd_ = -1;
  PingCommand cmd_;
  struct iovec io_rvec_, io_wvec_;
  msghdr msg_hdr_[2];
};

void RedisConnection::StartPolling(int fd, Proactor* mgr) {
  SubmitEntry sqe = GetEntry(fd, mgr);
  sqe.PrepPollAdd(fd, POLLIN);
  fd_ = fd;

  if (FLAGS_linked_sqe) {
    LOG(FATAL) << "TBD";
    // sqe->flags |= IOSQE_IO_LINK;
    // sqe->user_data = 0;
    InitiateRead(mgr);
  } else {
    state_ = WAIT_READ;
  }
}

void RedisConnection::InitiateRead(Proactor* mgr) {
  SubmitEntry se = GetEntry(fd_, mgr);
  auto rb = cmd_.read_buffer();
  io_rvec_.iov_base = rb.data();
  io_rvec_.iov_len = rb.size();

  se.PrepRecvMsg(fd_, &msg_hdr_[0], 0);
  state_ = READ;
}

void RedisConnection::InitiateWrite(Proactor* mgr) {
  //SubmitEntry se = GetEntry(fd_, mgr);
  auto rb = cmd_.reply();
  io_wvec_.iov_base = const_cast<void*>(rb.data());
  io_wvec_.iov_len = rb.size();
  CHECK_EQ(rb.size(), sendmsg(fd_, msg_hdr_ + 1, 0));

  SubmitEntry se = GetEntry(fd_, mgr);
  se.PrepPollAdd(fd_, POLLIN);
  state_ = WAIT_READ;
  return;

#if 0
  // On my tests io_uring_prep_sendmsg is much faster than io_uring_prep_writev and
  // subsequently sendmsg is faster than write.
  se.PrepSendMsg(fd_, msg_hdr_ + 1, 0);
  if (FLAGS_linked_sqe) {
    LOG(FATAL) << "TBD";
    /*sqe->flags |= IOSQE_IO_LINK;
    sqe->user_data = 0;

    event->AddPollin(mgr);
    state_ = WAIT_READ;*/
  } else {
    state_ = WRITE;
  }
#endif
}

void RedisConnection::Handle(IoResult res, int32_t payload, Proactor* proactor) {
  int socket = payload;
  DVLOG(1) << "RedisConnection::Handle [" << socket << "] state/res: " << state_ << "/" << res;

  switch (state_) {
    case WAIT_READ:
      CHECK_GT(res, 0) << strerror(-res) << socket;
      InitiateRead(proactor);
      break;
    case READ:
      if (res > 0) {
        if (cmd_.Decode(res)) {  // The flow has a bug in case of pipelined requests.
          DVLOG(1) << "Sending PONG to " << socket;
          ping_qps.Inc();
          InitiateWrite(proactor);
        } else {
          InitiateRead(proactor);
        }
      } else {          // res <= 0
        if (res < 0) {  // 0 means EOF (socket closed).
          char* str = strerror(-res);
          LOG(WARNING) << "Socket error " << -res << "/" << str;
        }

        // In any case we close the connection.
        VLOG(1) << "Closing client socket " << socket;
        shutdown(socket, SHUT_RDWR);  // To send FYN as gentlemen would do.
        close(socket);
      }
      break;
    case WRITE:
      CHECK_GT(res, 0);
      SubmitEntry se = GetEntry(fd_, proactor);
      se.PrepPollAdd(fd_, POLLIN);
      state_ = WAIT_READ;
      break;
  }
}

void HandleAccept(IoResult res, int32_t payload, Proactor* mgr) {
  struct sockaddr_in client_addr;
  socklen_t len = sizeof(client_addr);
  int32_t socket = payload;

  // res is a mask of POLLXXX constants, see revents in poll(2) for more information.
  CHECK_GT(res, 0) << strerror(-res);
  if (res == POLLERR) {
    CHECK_EQ(0, close(socket));
    return;
  }
  VLOG(1) << "Completion HandleAccept " << res;

  while (true) {
    // We could remove accept4 in favor of uring but since it's relateively uncommong operation
    // we do not care.
    int conn_fd = accept4(socket, (struct sockaddr*)&client_addr, &len, SOCK_NONBLOCK);
    if (conn_fd == -1) {
      if (errno == EAGAIN)
        break;
      char* str = strerror(errno);
      LOG(FATAL) << "Error calling accept4 " << errno << "/" << str;
    }
    CHECK_GT(conn_fd, 0);

    std::shared_ptr<RedisConnection> connection(new RedisConnection);
    connection->StartPolling(conn_fd, mgr);

    VLOG(2) << "Accepted " << conn_fd;
  }

  SubmitEntry se = mgr->GetSubmitEntry(&HandleAccept, socket);
  se.PrepPollAdd(socket, POLLIN);  // resend it.
}

// Consider returning -res for errors.
int posix_err_wrap(int res, boost::system::error_code* ec) {
  if (res < 0) {
    *ec = boost::system::error_code(errno, boost::asio::error::get_system_category());
  }
  return res;
}

#if 0
class ServerSocket {
  ServerSocket(const ServerSocket&) = delete;
  void operator=(const ServerSocket&) = delete;

 public:
  using native_handle_type = int;

  ServerSocket() : fd_event_(nullptr) {
  }

  ServerSocket(FD int fd, Proactor* proactor);

  ServerSocket(ServerSocket&& other) noexcept : fd_event_(other.fd_event_) {
    other.fd_event_ = nullptr;
  }

  ~ServerSocket() {
    ::boost::system::error_code ec;
    Close(ec);  // Quietly close.

    LOG_IF(WARNING, ec) << "Error closing socket " << ec;
  }

  ::boost::system::error_code Accept(Proactor* proactor, ServerSocket* peer);

  ServerSocket& operator=(ServerSocket&& other) {
    if (fd_ > 0) {
      ::boost::system::error_code ec;
      Close(ec);
      LOG_IF(WARNING, ec) << "Error closing socket " << ec;
      fd_ = -1;
    }
    std::swap(fd_, other.fd_);
  }

  void Close(::boost::system::error_code& ec) {
    if (fd_ > 0) {
      int res = posix_err_wrap(::close(fd_), &ec);
      fd_ = -1;
    }
  }

  native_handle_type native_handle() const {
    return fd_;
  }

 private:
  FdEvent* fd_event_;
};


auto BuildUringFiberCallback(FdEvent::IoResult* res) {
  return [res, me = fibers::context::active()](FdEvent::IoResult io_res, Proactor*, FdEvent*) {
    *res = io_res;
    fibers::context::active()->schedule(me);  // Awake pending fiber.
  };
}

void ServerSocket::Accept(ServerSocket& peer, ::boost::system::error_code& ec) {
  ::boost::asio::ip::tcp::endpoint endpoint;
  socklen_t addr_len = endpoint.capacity();

  while (true) {
    int res = accept4(fd_, endpoint.data(), &addr_len, SOCK_NONBLOCK | SOCK_CLOEXEC);
    if (res > 0) {
      peer = ServerSocket{res};
      return;
    }
    if (res == -1 && errno == EAGAIN) {
      FdEvent::IoResult io_res = 0;
      auto cb = BuildUringFiberCallback(&io_res);

    }
  }
#endif

void ManageAcceptions(int sock_listen_fd, Proactor* proactor) {
  fibers::context* me = fibers::context::active();
  UringFiberProps* props = reinterpret_cast<UringFiberProps*>(me->get_properties());
  CHECK(props);
  props->set_name("Acceptions");

  struct sockaddr_in client_addr;
  socklen_t len = sizeof(client_addr);
  IoResult completion_result = 0;

  auto cb = [me, &completion_result](IoResult res, int32_t, Proactor* mgr) {
    completion_result = res;
    fibers::context::active()->schedule(me);
  };

  while (true) {
    SubmitEntry se = proactor->GetSubmitEntry(cb, 0);
    se.PrepPollAdd(sock_listen_fd, POLLIN);
    me->suspend();

    if (completion_result == POLLERR) {
      CHECK_EQ(0, close(sock_listen_fd));
      break;
    }
    VLOG(1) << "Got accept completion " << completion_result;

    // We could remove accept4 in favor of uring but since it's relateively uncommong operation
    // we do not care.
    int conn_fd;
    while (true) {
      conn_fd = accept4(sock_listen_fd, (struct sockaddr*)&client_addr, &len, SOCK_NONBLOCK);
      if (conn_fd == -1)
        break;
      CHECK_GT(conn_fd, 0);

      std::shared_ptr<RedisConnection> connection(new RedisConnection);
      connection->StartPolling(conn_fd, proactor);

      VLOG(2) << "Accepted " << conn_fd;
    }

    if (errno != EAGAIN) {
      char* str = strerror(errno);
      LOG(FATAL) << "Error calling accept4 " << errno << "/" << str;
    }
  }
}

int main(int argc, char* argv[]) {
  MainInitGuard guard(&argc, &argv);

  CHECK_GT(FLAGS_port, 0);

  IoContextPool pool{1};
  pool.Run();
  AcceptServer accept_server(&pool);
  http::Listener<> http_listener;

  if (FLAGS_http_port >= 0) {
    uint16_t port = accept_server.AddListener(FLAGS_http_port, &http_listener);
    LOG(INFO) << "Started http server on port " << port;
    accept_server.Run();
  }

  int sock_listen_fd = SetupListenSock(FLAGS_port);
  Proactor proactor{FLAGS_queue_depth};

  accept_server.TriggerOnBreakSignal([&] {
    shutdown(sock_listen_fd, SHUT_RDWR);
    // close(sock_listen_fd);
    proactor.Stop();
  });

  std::thread t1([&] { proactor.Run(); });
  if (false) {
    proactor.AsyncFiber([&] {
      SubmitEntry se = proactor.GetSubmitEntry(&HandleAccept, sock_listen_fd);
      se.PrepPollAdd(sock_listen_fd, POLLIN);
    });
  } else {
    proactor.AsyncFiber(&ManageAcceptions, sock_listen_fd, &proactor);
  }
  t1.join();
  accept_server.Stop(true);

  return 0;
}
