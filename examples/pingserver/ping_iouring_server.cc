// Copyright 2020, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "base/init.h"
#include "examples/pingserver/ping_command.h"
#include "util/asio/accept_server.h"
#include "util/asio/io_context_pool.h"
#include "util/uring/proactor.h"
#include "util/http/http_conn_handler.h"
#include "util/stats/varz_stats.h"

using namespace util;
using namespace uring;

DEFINE_int32(http_port, 8080, "Http port.");
DEFINE_int32(port, 6380, "Redis port");
DEFINE_bool(linked_ske, false, "If true, then no-op events are linked to the next ones");

VarzQps ping_qps("ping-qps");

static int SetupListenSock(int port) {
  struct sockaddr_in server_addr;
  int fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
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

  void Handle(FdEvent::IoResult res, Proactor* mgr, FdEvent* event);

  void StartPolling(int fd, Proactor* mgr);

 private:
  enum State { WAIT_READ, READ, WRITE } state_ = WAIT_READ;
  void InitiateRead(Proactor* mgr, FdEvent* event);
  void InitiateWrite(Proactor* mgr, FdEvent* event);

  PingCommand cmd_;
  struct iovec io_rvec_, io_wvec_;
  msghdr msg_hdr_[2];
};

void RedisConnection::StartPolling(int fd, Proactor* mgr) {
  auto ptr = shared_from_this();

  auto cb = [ptr = std::move(ptr)](int32_t res, Proactor* mgr, FdEvent* me) {
    ptr->Handle(res, mgr, me);
  };

  FdEvent* event = mgr->GetFdEvent(fd);
  event->Arm(std::move(cb));

  struct io_uring_sqe* sqe = mgr->GetSubmitEntry();
  io_uring_prep_poll_add(sqe, event->handle(), POLLIN);

  if (FLAGS_linked_ske) {
    sqe->flags |= IOSQE_IO_LINK;
    sqe->user_data = 0;
    InitiateRead(mgr, event);
  } else {
    io_uring_sqe_set_data(sqe, event);
    state_ = WAIT_READ;
  }
}

void RedisConnection::InitiateRead(Proactor* mgr, FdEvent* event) {
  int socket = event->handle();
  io_uring_sqe* sqe = mgr->GetSubmitEntry();
  auto rb = cmd_.read_buffer();
  io_rvec_.iov_base = rb.data();
  io_rvec_.iov_len = rb.size();

  io_uring_prep_recvmsg(sqe, socket, &msg_hdr_[0], 0);
  io_uring_sqe_set_data(sqe, event);
  state_ = READ;
}

void RedisConnection::InitiateWrite(Proactor* mgr, FdEvent* event) {
  int socket = event->handle();
  io_uring_sqe* sqe = mgr->GetSubmitEntry();
  auto rb = cmd_.reply();

  io_wvec_.iov_base = const_cast<void*>(rb.data());
  io_wvec_.iov_len = rb.size();

  // On my tests io_uring_prep_sendmsg is much faster than io_uring_prep_writev and
  // subsequently sendmsg is faster than write.
  io_uring_prep_sendmsg(sqe, socket, &msg_hdr_[1], 0);
  if (FLAGS_linked_ske) {
    sqe->flags |= IOSQE_IO_LINK;
    sqe->user_data = 0;

    event->AddPollin(mgr);
    state_ = WAIT_READ;
  } else {
    io_uring_sqe_set_data(sqe, event);
    state_ = WRITE;
  }
}

void RedisConnection::Handle(FdEvent::IoResult res, Proactor* mgr, FdEvent* event) {
  int socket = event->handle();
  DVLOG(1) << "RedisConnection::Handle [" << socket << "] state/res: " << state_ << "/" << res;

  switch (state_) {
    case WAIT_READ:
      CHECK_GT(res, 0);
      InitiateRead(mgr, event);
      break;
    case READ:
      if (res > 0) {
        if (cmd_.Decode(res)) {  // The flow has a bug in case of pipelined requests.
          DVLOG(1) << "Sending PONG to " << socket;
          ping_qps.Inc();
          InitiateWrite(mgr, event);
        } else {
          InitiateRead(mgr, event);
        }
      } else {          // res <= 0
        if (res < 0) {  // 0 means EOF (socket closed).
          char* str = strerror(-res);
          LOG(WARNING) << "Socket error " << -res << "/" << str;
        }

        // In any case we close the connection.
        shutdown(socket, SHUT_RDWR);  // To send FYN as gentlemen would do.
        close(socket);
        event->DisarmAndDiscard(mgr);  // After this moment 'this' is deleted.
      }
      break;
    case WRITE:
      CHECK_GT(res, 0);
      state_ = WAIT_READ;
      event->AddPollin(mgr);
      break;
  }
}

void HandleAccept(FdEvent::IoResult res, Proactor* mgr, FdEvent* me) {
  struct sockaddr_in client_addr;
  socklen_t len = sizeof(client_addr);

  // TBD: to understand what res means here. Maybe, number of bytes available?
  CHECK_GT(res, 0) << strerror(-res);
  VLOG(1) << "Completion HandleAccept " << res;

  while (true) {
    // We could remove accept4 in favor of uring but since it's relateively uncommong operation
    // we do not care.
    int conn_fd = accept4(me->handle(), (struct sockaddr*)&client_addr, &len, SOCK_NONBLOCK);
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
  me->AddPollin(mgr);  // resend it.
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
  Proactor proactor;

  accept_server.TriggerOnBreakSignal([&] { proactor.Stop();});

  FdEvent* event = proactor.GetFdEvent(sock_listen_fd);
  event->Arm(&HandleAccept);
  event->AddPollin(&proactor);

  std::thread t1([&] { proactor.Run(); });
  t1.join();
  accept_server.Stop(true);

  return 0;
}
