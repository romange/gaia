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

FiberQueueThreadPool::FiberQueueThreadPool(unsigned num_threads, unsigned queue_size) {
  if (num_threads == 0) {
    num_threads = std::thread::hardware_concurrency();
  }
  worker_size_ = num_threads;
  workers_.reset(new Worker[num_threads]);

  for (unsigned i = 0; i < num_threads; ++i) {
    string name = absl::StrCat("fq_pool", i);

    auto fn = std::bind(&FiberQueueThreadPool::WorkerFunction, this, i);
    workers_[i].q.reset(new FuncQ(queue_size));
    workers_[i].tid = base::StartThread(name.c_str(), fn);
  }
}

FiberQueueThreadPool::~FiberQueueThreadPool() {
  VLOG(1) << "FiberQueueThreadPool::~FiberQueueThreadPool";

  Shutdown();
}

void FiberQueueThreadPool::Shutdown() {
  if (!workers_)
    return;

  is_closed_.store(true, std::memory_order_seq_cst);
  for (size_t i = 0; i < worker_size_; ++i) {
    workers_[i].pull_ec.notifyAll();
  }
  Func f;

  for (size_t i = 0; i < worker_size_; ++i) {
    auto& w = workers_[i];
    pthread_join(w.tid, nullptr);
    CHECK(!w.q->try_dequeue(f));
  }

  workers_.reset();
  VLOG(1) << "FiberQueueThreadPool::ShutdownEnd";
}

void FiberQueueThreadPool::WorkerFunction(unsigned index) {
  bool is_closed = false;
  Func f;
  Worker& me = workers_[index];
  auto cb = [&]() {
    if (me.q->try_dequeue(f)) {
      me.push_ec.notify();
      return true;
    }

    if (is_closed_.load(std::memory_order_acquire)) {
      is_closed = true;
      return true;
    }
    return false;
  };

  while (true) {
    me.pull_ec.await(cb);

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
