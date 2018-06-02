// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include <cassert>
#include <iostream>
#include <memory>

#include "base/init.h"
#include "base/integral_types.h"
#include "base/logging.h"

#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/executors/GlobalExecutor.h>
#include <folly/io/async/EventBase.h>
#include <folly/fibers/FiberManager.h>
#include <folly/fibers/FiberManagerMap.h>
#include <folly/File.h>
#include <folly/FileUtil.h>
#include <folly/init/Init.h>

using namespace std;
using namespace std::chrono_literals;

DEFINE_int32(count, 1000, "");
DEFINE_int32(threads, 4, "");
DEFINE_string(file, "", "File to read asynchronously");

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
using namespace folly;

void TestFileRead(fibers::FiberManager* fb, File file) {
  auto executor = getCPUExecutor();
  ssize_t total = 0;
  std::unique_ptr<uint8_t[]> buf(new uint8_t[1 << 16]);

  while (true) {
    // Promise and Baton are thread-safe.
    ssize_t rc = fibers::await([&](fibers::Promise<ssize_t> rc_promise) {
      executor->add([&, dest = buf.get(), rc_promise = std::move(rc_promise)] ()
        mutable {
        ssize_t rc = folly::readNoInt(file.fd(), dest, 1 << 16);
        rc_promise.setValue(rc);
        });  // executor
    });  // await

    if (rc <= 0) {
      if (rc < 0)
        cout << "Error " << rc << endl;
      break;
    }
    total += rc;
    //
  }
  cout << "Read " << total << " bytes " << endl;
}

int main(int argc, char **argv) {
  // MainInitGuard guard(&argc, &argv);
  folly::init(&argc, &argv);

  folly::EventBase evb;
  folly::fibers::FiberManager::Options opts;

  // Does not really bound number of active fibers, probably limits number of "recycled" ones.
  opts.maxFibersPoolSize = 10;

  auto diskIOThreadPool = std::make_shared<folly::CPUThreadPoolExecutor>(
    FLAGS_threads, std::make_shared<folly::NamedThreadFactory>("DiskIOThread"));
  folly::setCPUExecutor(diskIOThreadPool);

  /*fibers::Baton baton2;
  CHECK(!baton2.try_wait());
  baton2.post();
  baton2.wait();
  CHECK(!baton2.try_wait());*/

  auto& fiberManager = folly::fibers::getFiberManager(evb);
  folly::fibers::Baton baton;

  for (unsigned i = 0; i < 5000; ++i) {
    fiberManager.addTask([i] {
      // std::cout << "Task " << i << ": start" << std::endl;
    });
  }
  fiberManager.addTask([] {
    std::cout << "Task 1: start" << std::endl;
    auto start = steady_clock::now();
    SleepFiber(2s);
    std::cout << "Task 1: after Sleep, " << as_millis(steady_clock::now() - start) << endl;
  });
  fiberManager.addTask([] {
    int i = fibers::Promise<int>::await([](fibers::Promise<int> p) { p.setValue(5);});
    std::cout << "Promise " << i << endl;
  });

  if (!FLAGS_file.empty()) {
    auto start = steady_clock::now();

    fiberManager.addTask([&] {
      TestFileRead(&fiberManager, folly::File(FLAGS_file));
      std::cout << "File read took : " << as_millis(steady_clock::now() - start) << endl;
    });

    TestFileRead(&fiberManager, folly::File(FLAGS_file));
    std::cout << "File SyncRead took : " << as_millis(steady_clock::now() - start) << endl;
  }

  /*fiberManager.addTask([&baton] {
    std::cout << "Task 2: start" << std::endl;
    baton.post();
    std::cout << "Task 2: after baton.post()" << std::endl;
  });*/
  std::cout << "Before Loop " << fiberManager.fibersAllocated() << "\n";
  evb.loop();

  return 0;
}
