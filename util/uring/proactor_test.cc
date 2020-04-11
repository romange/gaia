// Copyright 2020, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/uring/proactor.h"

#include "base/gtest.h"
#include "base/logging.h"

using namespace boost;

namespace util {
namespace uring {

class ProactorTest : public testing::Test {
 protected:
  void SetUp() override {
    proactor_ = std::make_unique<Proactor>();
  }

  void TearDown() {
    proactor_.reset();
  }

  static void SetUpTestCase() {
  }

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

TEST_F(ProactorTest, AsyncEvent) {
  bool event_fired = false;

  auto cb = [&](FdEvent::IoResult, Proactor*, FdEvent*) {
    event_fired = true;
    proactor_->Stop();
  };

  std::thread t([&] { proactor_->Run(); });
  proactor_->Async([&] {
    FdEvent* event = proactor_->GetFdEvent(-1);
    event->Arm(cb);

    io_uring_sqe* sqe = proactor_->GetSubmitEntry();
    io_uring_prep_nop(sqe);
    sqe->user_data = uint64_t(event);
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
