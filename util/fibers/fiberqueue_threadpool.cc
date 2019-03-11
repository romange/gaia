// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "util/fibers/fiberqueue_threadpool.h"

#include "absl/strings/str_cat.h"
#include "base/pthread_utils.h"

namespace util {
namespace fibers_ext {
using namespace boost;
using namespace std;

FiberQueueThreadPool::FiberQueueThreadPool(unsigned num_threads, unsigned queue_size)
    : q_(queue_size) {
  if (num_threads == 0) {
    num_threads = std::thread::hardware_concurrency();
  }

  for (unsigned i = 0; i < num_threads; ++i) {
    string name = absl::StrCat("sq_threadpool", i);

    auto fn = std::bind(&FiberQueueThreadPool::WorkerFunction, this);
    workers_.emplace_back(base::StartThread(name.c_str(), fn));
  }
}

FiberQueueThreadPool::~FiberQueueThreadPool() {
  VLOG(1) << "FiberQueueThreadPool::~FiberQueueThreadPool";

  if (!workers_.empty()) {
    Shutdown();
  }
}

void FiberQueueThreadPool::Shutdown() {
  is_closed_.store(true, std::memory_order_seq_cst);
  pull_ec_.notifyAll();
  for (auto& w : workers_) {
    pthread_join(w, nullptr);
  }
  Func f;
  CHECK(!q_.try_dequeue(f));
  workers_.clear();
  VLOG(1) << "FiberQueueThreadPool::ShutdownEnd";
}

void FiberQueueThreadPool::WorkerFunction() {
  bool is_closed = false;
  Func f;

  auto cb = [&]() {
    if (q_.try_dequeue(f)) {
      push_ec_.notify();
      return true;
    }

    if (is_closed_.load(std::memory_order_acquire)) {
      is_closed = true;
      return true;
    }
    return false;
  };

  while (true) {
    pull_ec_.await(cb);

    if (is_closed)
      break;
    try {
      f();
    } catch (std::exception& e) {
      // std::exception_ptr p = std::current_exception();
      LOG(FATAL) << "Exception " << e.what();
    }
  }
  VLOG(1) << "FiberQueueThreadPool::Exit";
}

}  // namespace fibers_ext
}  // namespace util
