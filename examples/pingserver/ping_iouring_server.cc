// Copyright 2020, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include <liburing.h>

#include "base/init.h"
#include "examples/pingserver/ping_command.h"
#include "util/asio/accept_server.h"
#include "util/asio/io_context_pool.h"
#include "util/http/http_conn_handler.h"

#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>

#include <stdlib.h>

using namespace util;

DEFINE_int32(http_port, 8080, "Http port.");
DEFINE_int32(port, 6380, "Redis port");

static int SetupListenSock(int port) {
  struct sockaddr_in server_addr;
  int fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
  CHECK_GT(fd, 0);

  memset(&server_addr, 0, sizeof(server_addr));
  server_addr.sin_family = AF_INET;
  server_addr.sin_port = htons(FLAGS_port);
  server_addr.sin_addr.s_addr = INADDR_ANY;

  constexpr uint32_t BACKLOG = 128;

  CHECK_EQ(0, bind(fd, (struct sockaddr*)&server_addr, sizeof(server_addr)));
  CHECK_EQ(0, listen(fd, BACKLOG));

  return fd;
}

class URingManager;
class URingEvent;

// first argument is the result of completion operation: io_uring_cqe.res
using CbType = std::function<void(int32_t, URingManager*, URingEvent*)>;

class URingEvent {
  int fd_ = -1;
  CbType cb_;  // This lambda might hold auxillary data that is needed to run.

 public:
  explicit URingEvent(int fd) : fd_(fd) {
  }

  void Set(CbType cb) {
    cb_ = std::move(cb);
  }

  int fd() const {
    return fd_;
  }
  void Run(int res, URingManager* mgr) {
    cb_(res, mgr, this);
  }
};

class URingManager {
  struct io_uring ring_;
  std::vector<std::unique_ptr<URingEvent>> storage_;

 public:
  URingManager();
  ~URingManager();

  void Arm(int fd, CbType cb);

  io_uring_sqe* GetSubmit() {
    io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    CHECK(sqe) << "TBD: To handle ring overflow";

    return sqe;
  }

  void RearmEvent(URingEvent* event) {
    struct io_uring_sqe* sqe = GetSubmit();

    io_uring_prep_poll_add(sqe, event->fd(), POLLIN);
    io_uring_sqe_set_data(sqe, event);
  }

  void Run();
};

URingManager::URingManager() {
  struct io_uring_params params;
  memset(&params, 0, sizeof(params));
  CHECK_EQ(0, io_uring_queue_init_params(4096, &ring_, &params));
}

URingManager::~URingManager() {
  io_uring_queue_exit(&ring_);
}

void URingManager::Arm(int fd, CbType cb) {
  URingEvent* event = new URingEvent(fd);
  event->Set(std::move(cb));
  storage_.emplace_back(event);

  RearmEvent(event);
}


void URingManager::Run() {
  io_uring_cqe* cqe = nullptr;
  struct io_uring_cqe* cqes[32];
  static_assert(32 == arraysize(cqes), "");
  while (true) {
    // tell kernel we have put a sqe on the submission ring.
    // Might return negative -errno.
    CHECK_GE(io_uring_submit(&ring_), 0);  // Could be combined into io_uring_submit_and_wait.

    // wait for new cqe to become available
    int res = io_uring_wait_cqe(&ring_, &cqe);  // res must be <= 0.
    if (res < 0) {
      if (-res == EINTR)
        break;

      char* str = strerror(-res);
      LOG(FATAL) << "Error " << -res << "/" << str;
    }
    CHECK_EQ(0, res);

    // check how many cqe's are on the cqe ring, and put these cqe's in an array
    unsigned cqe_count = io_uring_peek_batch_cqe(&ring_, cqes, arraysize(cqes));

    for (unsigned i = 0; i < cqe_count; ++i) {
      struct io_uring_cqe* cqe = cqes[i];
      auto res = cqe->res;
      // auto flags = cqe->flags;  cqe->flags is currently unused.

      URingEvent* event = reinterpret_cast<URingEvent*>(io_uring_cqe_get_data(cqe));
      CHECK(event);
      io_uring_cq_advance(&ring_, 1);  // Once we copied the data we can mark the cqe consumed.

      event->Run(res, this);
    }
  }
}

class RedisConnection {
 public:
  RedisConnection() {
  }

  void Handle(int32_t res, URingManager* mgr, URingEvent* event);

 private:
  enum State { WAIT_READ, READ, WRITE } state_ = WAIT_READ;
  void InitiateRead(URingManager* mgr, URingEvent* event);
  void InitiateWrite(URingManager* mgr, URingEvent* event);

  PingCommand cmd_;
  struct iovec io_rvec_, io_wvec_;
};

void RedisConnection::InitiateRead(URingManager* mgr, URingEvent* event) {
  int socket = event->fd();
  io_uring_sqe* sqe = mgr->GetSubmit();
  auto rb = cmd_.read_buffer();
  io_rvec_.iov_base = rb.data();
  io_rvec_.iov_len = rb.size();
  io_uring_prep_readv(sqe, socket, &io_rvec_, 1, 0);
  io_uring_sqe_set_data(sqe, event);
  state_ = READ;
}

void RedisConnection::InitiateWrite(URingManager* mgr, URingEvent* event) {
  int socket = event->fd();
  io_uring_sqe* sqe = mgr->GetSubmit();
  auto rb = cmd_.reply();
  io_wvec_.iov_base = const_cast<void*>(rb.data());
  io_wvec_.iov_len = rb.size();
  io_uring_prep_writev(sqe, socket, &io_wvec_, 1, 0);
  io_uring_sqe_set_data(sqe, event);
  state_ = WRITE;
}

void RedisConnection::Handle(int32_t res, URingManager* mgr, URingEvent* event) {
  int socket = event->fd();
  DVLOG(1) << "RedisConnection::Handle [" << socket << "] " << state_ << "/" << res;

  switch (state_) {
    case WAIT_READ:
      CHECK_GT(res, 0);
      InitiateRead(mgr, event);
      break;
    case READ:
      if (res > 0) {
        if (cmd_.Decode(res)) {  // It has a bug in case of pipelined requests.
          DVLOG(1) << "Sending PONG to " << socket;
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
        event->Set(nullptr);  // After this moment 'this' is deleted.
      }
      break;
    case WRITE:
      CHECK_GT(res, 0);
      state_ = WAIT_READ;
      mgr->RearmEvent(event);
      break;
  }
}

void HandleAccept(int32_t res, URingManager* mgr, URingEvent* me) {
  struct sockaddr_in client_addr;
  socklen_t len = sizeof(client_addr);

  // TBD: to understand what res means here. Maybe, number of bytes available?
  CHECK_GT(res, 0);
  VLOG(1) << "Completion HandleAccept " << res;

  while (true) {
    // We could remove accept4 in favor of uring but since it's relateively uncommong operation
    // we do not care.
    int conn_fd = accept4(me->fd(), (struct sockaddr*)&client_addr, &len, SOCK_NONBLOCK);
    if (conn_fd == -1) {
      if (errno == EAGAIN)
        break;
      char* str = strerror(errno);
      LOG(FATAL) << "Error calling accept4 " << errno << "/" << str;
    }
    CHECK_GT(conn_fd, 0);

    std::shared_ptr<RedisConnection> connection(new RedisConnection);
    auto cb = [c = std::move(connection)](int32_t res, URingManager* mgr, URingEvent* me) {
      c->Handle(res, mgr, me);
    };

    mgr->Arm(conn_fd, std::move(cb));

    VLOG(2) << "Accepted " << conn_fd;
  }
  mgr->RearmEvent(me);  // TODO: to test if we need to do it.
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
  URingManager mgr;
  mgr.Arm(sock_listen_fd, &HandleAccept);
  mgr.Run();

#if 0
      if (type == READ) {
        int bytes_read = cqe->res;
        if (bytes_read <= 0) {
          // no bytes available on socket, client must be disconnected
          io_uring_cqe_seen(&ring, cqe);
          shutdown(user_data->fd, SHUT_RDWR);
        } else {
          // bytes have been read into iovec, add write to socket sqe
          io_uring_cqe_seen(&ring, cqe);
          add_socket_write(&ring, user_data->fd, bytes_read, WRITE);
        }
      } else if (type == WRITE) {
        // write to socket completed, re-add poll sqe
        io_uring_cqe_seen(&ring, cqe);
        add_poll(&ring, user_data->fd, POLL_NEW_CONNECTION);
      }
    }
  }
#endif
}
