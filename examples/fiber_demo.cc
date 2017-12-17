#include <cassert>
#include <iostream>
#include <memory>
#include "base/init.h"
#include "base/integral_types.h"
#include "base/logging.h"

#include <folly/io/async/EventBase.h>
#include <folly/fibers/FiberManager.h>
#include <folly/fibers/FiberManagerMap.h>

using namespace std;
using namespace std::chrono_literals;

DEFINE_int32(count, 1000, "");

template<typename Rep, typename Period> void SleepFiber(
const std::chrono::duration<Rep, Period>& timeout) {
  folly::fibers::Baton baton;
  baton.timed_wait(timeout);
}

template<typename Rep, typename Period>
  constexpr uint32_t as_millis(const std::chrono::duration<Rep, Period>& dur) {
    return chrono::duration_cast<chrono::milliseconds>(dur).count();
}

using std::chrono::steady_clock;
int main(int argc, char **argv) {
  MainInitGuard guard(&argc, &argv);

  folly::EventBase evb;
  folly::fibers::FiberManager::Options opts;
  opts.maxFibersPoolSize = 10;

  auto& fiberManager = folly::fibers::getFiberManager(evb);
  folly::fibers::Baton baton;

  for (unsigned i = 0; i < 5000; ++i) {
    fiberManager.addTask([i] {
      // std::cout << "Task " << i << ": start" << std::endl;
    });
  }
  fiberManager.addTask([&baton] {
    std::cout << "Task 1: start" << std::endl;
    auto start = steady_clock::now();
    SleepFiber(2s);
    std::cout << "Task 1: after Sleep, " << as_millis(steady_clock::now() - start) << endl;
  });

  /*fiberManager.addTask([&baton] {
    std::cout << "Task 2: start" << std::endl;
    baton.post();
    std::cout << "Task 2: after baton.post()" << std::endl;
  });*/
  std::cout << "Before Loop " << fiberManager.fibersAllocated() << "\n";
  evb.loop();

  return 0;
}
