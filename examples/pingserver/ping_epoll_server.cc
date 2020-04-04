// Copyright 2020, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include <sys/epoll.h>

#include "base/init.h"
#include "examples/pingserver/ping_command.h"
#include "util/asio/accept_server.h"
#include "util/asio/io_context_pool.h"
#include "util/http/http_conn_handler.h"

DEFINE_int32(http_port, 8080, "Http port.");
DEFINE_int32(port, 6380, "Redis port");

using namespace util;

int SetupListenSock(int port) {
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

class EpollManager;
class EpollWrapper;

using CbType = std::function<void(EpollManager*, EpollWrapper*)>;

class EpollWrapper {
  int fd_ = -1;
  CbType cb_;

 public:
  explicit EpollWrapper(int fd) : fd_(fd) {
  }

  void Set(CbType cb) {
    cb_ = std::move(cb);
  }

  int fd() const {
    return fd_;
  }
  void Run(EpollManager* mgr) {
    cb_(mgr, this);
  }
};

class EpollManager {
  static constexpr uint32_t MAX_EVENTS = 128;
  struct epoll_event events_[MAX_EVENTS];
  int epoll_fd_;
  std::vector<std::unique_ptr<EpollWrapper>> storage_;

 public:
  EpollManager();
  ~EpollManager();

  void Arm(uint32_t mask, int fd, std::function<void(EpollManager*, EpollWrapper*)> cb);
  void Disarm(int fd);

  void Run();
};

EpollManager::EpollManager() {
  epoll_fd_ = epoll_create1(0);
  CHECK_GT(epoll_fd_, 0);
}

EpollManager::~EpollManager() {
  CHECK_EQ(0, close(epoll_fd_));
}

void EpollManager::Arm(uint32_t mask, int fd,
                       std::function<void(EpollManager*, EpollWrapper*)> cb) {
  struct epoll_event ev;

  EpollWrapper* wrapper = new EpollWrapper(fd);
  wrapper->Set(std::move(cb));

  ev.events = mask;
  ev.data.ptr = wrapper;

  storage_.emplace_back(wrapper);
  CHECK_EQ(0, epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &ev));
}

void EpollManager::Disarm(int fd) {
  CHECK_EQ(0, epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, NULL));
  shutdown(fd, SHUT_RDWR);  // To send FYN as gentlemen would do.
  close(fd);
}

void EpollManager::Run() {
  while (true) {
    int new_events = epoll_wait(epoll_fd_, events_, MAX_EVENTS, -1);
    if (new_events < 0) {
      if (errno == EINTR) {  // We got signal interrupt - just exit.
        break;
      } else {
        char* str = strerror(errno);
        LOG(FATAL) << "Error running epoll_wait " << errno << "/" << str;
      }
    }

    for (int i = 0; i < new_events; ++i) {
      EpollWrapper* wrapper = reinterpret_cast<EpollWrapper*>(events_[i].data.ptr);
      wrapper->Run(this);
    }
  }
}

class RedisConnection {
 public:
  RedisConnection() {
  }

  void Handle(EpollManager* mgr, EpollWrapper* wrapper);

 private:
  PingCommand cmd_;
};

void RedisConnection::Handle(EpollManager* mgr, EpollWrapper* wrapper) {
  auto rb = cmd_.read_buffer();
  int socket = wrapper->fd();

  DVLOG(1) << "Handling socket " << socket;

  while (true) {
    int res = read(socket, rb.data(), rb.size());
    if (res > 0) {
      if (cmd_.Decode(res)) {  // It has a bug in case of pipelined requests.
        DVLOG(1) << "Sending PONG to " << socket;

        int res = write(socket, cmd_.reply().data(), cmd_.reply().size());
        CHECK_GT(res, 0);
        #if 0
        if (res <= 0) {
          CHECK_EQ(-1, res);  // could it be 0 for sockets?
          if (errno != EAGAIN) {
            char* str = strerror(errno);
            LOG(FATAL) << "Error " << errno << "/" << str;
          }
        }
        #endif
      }
    } else {  // Error or EOF
      if (res < 0) {  // 0 means EOF (socket closed).
        CHECK_EQ(-1, res);
        if (errno == EAGAIN)
          return;
        char* str = strerror(errno);
        LOG(WARNING) << "Error " << errno << "/" << str;
      }

      // In any case we close the connection.
      mgr->Disarm(socket);
      wrapper->Set(nullptr);  // After this moment 'this' is deleted.
      break;
    }
  }
}

static unsigned num_opened = 0;

void HandleAccept(EpollManager* mgr, EpollWrapper* me) {
  struct sockaddr_in client_addr;
  socklen_t len = sizeof(client_addr);

  // We do not need to loop here because we subscribed HandleAccept with level-trigerred
  // event.
  int conn_fd = accept4(me->fd(), (struct sockaddr*)&client_addr, &len, SOCK_NONBLOCK);
  CHECK_GT(conn_fd, 0);
  ++num_opened;

  std::shared_ptr<RedisConnection> connection(new RedisConnection);
  auto cb = [c = std::move(connection)](EpollManager* mgr, EpollWrapper* me) {
    c->Handle(mgr, me);
  };

  // We subscribe connectiosn with edge-triggerred event.
  mgr->Arm(EPOLLIN | EPOLLET, conn_fd, std::move(cb));

  VLOG(1) << "Accepted " << conn_fd;
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

  // accept_server.TriggerOnBreakSignal([&] { stop = true; });

  int sock_listen_fd = SetupListenSock(FLAGS_port);

  EpollManager mgr;
  mgr.Arm(EPOLLIN, sock_listen_fd, &HandleAccept);
  mgr.Run();

  accept_server.Stop(true);

  return 0;
}
