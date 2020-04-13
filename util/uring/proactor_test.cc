// Copyright 2020, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/uring/proactor.h"

#include "base/gtest.h"
#include "base/logging.h"

using namespace boost;

namespace util {
namespace uring {
constexpr uint32_t kRingDepth = 16;

class ProactorTest : public testing::Test {
 protected:

  void SetUp() override {
    proactor_ = std::make_unique<Proactor>(kRingDepth);
  }

  void TearDown() {
    proactor_.reset();
  }

  static void SetUpTestCase() {
  }

  using IoResult = Proactor::IoResult;

  std::unique_ptr<Proactor> proactor_;
};

TEST_F(ProactorTest, AsyncCall) {
  std::thread t([&] { proactor_->Run(); });
  for (unsigned i = 0; i < 10000; ++i) {
    proactor_->Async([] {});
  }
  usleep(5000);

  proactor_->Stop();
  t.join();
}


TEST_F(ProactorTest, SqeOverflowTBD) {
  std::thread t([&] { proactor_->Run(); });

  auto cb = [](IoResult, int64_t payload, Proactor*) {
    ASSERT_EQ(12051977, payload);
  };

  proactor_->Async([&] {
    for (unsigned i = 0; i < kRingDepth; ++i) {
      proactor_->GetSubmitEntry(cb, 12051977);
    }
  });

  proactor_->Stop();
  t.join();
}

TEST_F(ProactorTest, AsyncEvent) {
  bool event_fired = false;

  auto cb = [&event_fired](IoResult, int64_t payload, Proactor* p) {
    event_fired = true;
    p->Stop();
  };

  std::thread t([&] { proactor_->Run(); });
  proactor_->Async([&] {
    SubmitEntry se = proactor_->GetSubmitEntry(std::move(cb), 1);
    se.sqe()->opcode = IORING_OP_NOP;
  });
  t.join();

  EXPECT_TRUE(event_fired);
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
