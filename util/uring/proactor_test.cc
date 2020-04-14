// Copyright 2020, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/uring/proactor.h"

#include <fcntl.h>
#include <sys/socket.h>
#include <netinet/in.h>

#include "base/gtest.h"
#include "base/logging.h"
#include "util/uring/fiber_socket.h"
#include "util/uring/uring_fiber_algo.h"
#include "util/fibers/fibers_ext.h"

using namespace boost;
using namespace std;

namespace util {
namespace uring {

constexpr uint32_t kRingDepth = 16;

// Consider returning -res for errors.


static void ManageAcceptions(int sock_listen_fd, Proactor* proactor) {
  fibers::context* me = fibers::context::active();
  UringFiberProps* props = reinterpret_cast<UringFiberProps*>(me->get_properties());
  CHECK(props);
  props->set_name("Acceptions");

  FiberSocket fs(sock_listen_fd);

  while (true) {
    FiberSocket peer;
    std::error_code ec = fs.Accept(proactor, &peer);

    if (ec == errc::connection_aborted)
      break;
    if (ec) {
      LOG(FATAL) << "Error calling accept " << ec << "/" << ec.message();
    }
    VLOG(2) << "Accepted " << peer.native_handle();
  }
}

static int SetupListenSock(int port) {
  struct sockaddr_in server_addr;
  int fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
  CHECK_GT(fd, 0);
  const int val = 1;
  setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val));

  memset(&server_addr, 0, sizeof(server_addr));
  server_addr.sin_family = AF_INET;
  server_addr.sin_port = htons(port);
  server_addr.sin_addr.s_addr = INADDR_ANY;

  constexpr uint32_t BACKLOG = 128;

  CHECK_EQ(0, bind(fd, (struct sockaddr*)&server_addr, sizeof(server_addr)))
      << "Error: " << strerror(errno);
  CHECK_EQ(0, listen(fd, BACKLOG));

  return fd;
}


class ProactorTest : public testing::Test {
 protected:
  void SetUp() override {
    proactor_ = std::make_unique<Proactor>(kRingDepth);
    proactor_thread_ = thread{[&] { proactor_->Run(); }};
  }

  void TearDown() {
    proactor_->Stop();
    proactor_thread_.join();
    proactor_.reset();
  }

  static void SetUpTestCase() {
  }

  using IoResult = Proactor::IoResult;

  std::unique_ptr<Proactor> proactor_;
  std::thread proactor_thread_;
};

TEST_F(ProactorTest, AsyncCall) {
  for (unsigned i = 0; i < 10000; ++i) {
    proactor_->Async([] {});
  }
  usleep(5000);
}

TEST_F(ProactorTest, SqeOverflow) {
  size_t unique_id = 0;
  char buf[128];

  int fd = open(google::GetArgv0(), O_RDONLY | O_CLOEXEC);
  CHECK_GT(fd, 0);
  auto cb = [](IoResult, int64_t payload, Proactor*) {};

  proactor_->AsyncFiber([&] {
    for (unsigned i = 0; i < kRingDepth * 100; ++i) {
      SubmitEntry se = proactor_->GetSubmitEntry(cb, unique_id++);
      se.PrepRead(fd, buf, sizeof(buf), 0);
    }
  });

  close(fd);
}

TEST_F(ProactorTest, AsyncEvent) {
  fibers_ext::Done done;

  auto cb = [&done](IoResult, int64_t payload, Proactor* p) {
    done.Notify();
  };

  proactor_->Async([&] {
    SubmitEntry se = proactor_->GetSubmitEntry(std::move(cb), 1);
    se.sqe()->opcode = IORING_OP_NOP;
  });
  done.Wait();
}

TEST_F(ProactorTest, AcceptLoop) {
  int listen_fd = SetupListenSock(1234);
  proactor_->AsyncFiber(&ManageAcceptions, listen_fd, proactor_.get());
  usleep(1000);
  shutdown(listen_fd, SHUT_RDWR);
  close(listen_fd);
}

void BM_AsyncCall(benchmark::State& state) {
  Proactor proactor;
  std::thread t([&] { proactor.Run(); });

  while (state.KeepRunning()) {
    proactor.Async([] {});
  }
  proactor.Stop();
  t.join();
}
BENCHMARK(BM_AsyncCall);

}  // namespace uring
}  // namespace util
