// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include "base/mpmc_bounded_queue.h"
#include "util/fibers/fibers_ext.h"

namespace util {
namespace fibers_ext {

// This thread pool has a global fiber-friendly queue for incoming tasks.
class FiberQueueThreadPool {
 public:
  typedef std::function<void()> Func;

  explicit FiberQueueThreadPool(unsigned num_threads = 0, unsigned queue_size = 128);
  ~FiberQueueThreadPool();

  template <typename Func> auto Await(Func&& f) -> decltype(f()) {
    fibers_ext::Done done;
    using ResultType = decltype(f());
    detail::ResultMover<ResultType> mover;

    Add([&, f = std::forward<Func>(f)]() mutable {
      mover.Apply(f);
      done.Notify();
    });

    done.Wait();
    return std::move(mover).get();
  }

  template <typename F> void Add(F&& f) {
    while (true) {
      EventCount::Key key = push_ec_.prepareWait();
      if (q_.try_enqueue(std::forward<F>(f))) {
        pull_ec_.notify();
        break;
      }

      push_ec_.wait(key.epoch());
    }
  }

  void Shutdown();

 private:
  void WorkerFunction();

  std::vector<pthread_t> workers_;
  base::mpmc_bounded_queue<Func> q_;
  EventCount push_ec_, pull_ec_;
  std::atomic_bool is_closed_{false};
};

}  // namespace fibers_ext
}  // namespace util
