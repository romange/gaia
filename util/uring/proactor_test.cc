// Copyright 2020, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/uring/proactor.h"

#include "base/gtest.h"
#include "base/logging.h"

using namespace boost;

namespace util {
namespace uring {

static void print_action(int signal) {
  LOG(INFO) << "Got signal " << signal << " " << strsignal(signal);
}

class ProactorTest : public testing::Test {
 protected:
  void SetUp() override {
    proactor_ = std::make_unique<Proactor>();
  }

  void TearDown() {
    proactor_.reset();
  }

  static void SetUpTestCase() {
    sigset_t mask;
    sigfillset(&mask);
    sigdelset(&mask, SIGINT);
    sigdelset(&mask, SIGTERM);
    CHECK_EQ(0, pthread_sigmask(SIG_BLOCK, &mask, NULL));

    struct sigaction new_action;
    new_action.sa_handler = print_action;
    sigemptyset(&new_action.sa_mask);
    new_action.sa_flags = 0;

    sigaction(SIGINT, &new_action, NULL);
  }

  std::unique_ptr<Proactor> proactor_;
};

TEST_F(ProactorTest, AsyncCall) {
  std::thread t([&] { proactor_->Run(); });
  for (unsigned i = 0; i < 10000; ++i) {
    proactor_->Async([]{});
  }
  usleep(5000);

  proactor_->Stop();
  t.join();
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
