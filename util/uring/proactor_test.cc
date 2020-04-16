// Copyright 2020, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/uring/proactor.h"

#include <fcntl.h>
#include <sys/socket.h>
#include <netinet/in.h>

#include "base/gtest.h"
#include "base/logging.h"

#include "util/uring/accept_server.h"
#include "util/uring/uring_fiber_algo.h"
#include "util/fibers/fibers_ext.h"

using namespace boost;
using namespace std;

namespace util {
namespace uring {

constexpr uint32_t kRingDepth = 16;

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

TEST_F(ProactorTest, Await) {
  thread_local int val = 5;

  proactor_->Await([] { val = 15; });
  EXPECT_EQ(5, val);

  int j = proactor_->Await([] { return val; });
  EXPECT_EQ(15, j);
}

TEST_F(ProactorTest, SqeOverflow) {
  size_t unique_id = 0;
  char buf[128];

  int fd = open(google::GetArgv0(), O_RDONLY | O_CLOEXEC);
  CHECK_GT(fd, 0);
  auto cb = [](IoResult, int64_t payload, Proactor*) {};
  fibers_ext::Done done;

  proactor_->AsyncFiber([&, done] () mutable {
    for (unsigned i = 0; i < kRingDepth * 100; ++i) {
      SubmitEntry se = proactor_->GetSubmitEntry(cb, unique_id++);
      se.PrepRead(fd, buf, sizeof(buf), 0);
    }
    done.Notify();
  });

  done.Wait();
  close(fd);
}

TEST_F(ProactorTest, AsyncEvent) {
  fibers_ext::Done done;

  auto cb = [done](IoResult, int64_t payload, Proactor* p) mutable {
    done.Notify();
  };

  proactor_->Async([&] {
    SubmitEntry se = proactor_->GetSubmitEntry(std::move(cb), 1);
    se.sqe()->opcode = IORING_OP_NOP;
  });
  done.Wait();
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
