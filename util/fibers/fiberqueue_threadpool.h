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
    Done done;
    using ResultType = decltype(f());
    detail::ResultMover<ResultType> mover;

    Add([&, f = std::forward<Func>(f), done]() mutable {
      mover.Apply(f);
      done.Notify();
    });

    done.Wait();
    return std::move(mover).get();
  }

  template <typename Func> auto Await(size_t worker_index, Func&& f) -> decltype(f()) {
    Done done;
    using ResultType = decltype(f());
    detail::ResultMover<ResultType> mover;

    Add(worker_index, [&, f = std::forward<Func>(f), done]() mutable {
      mover.Apply(f);
      done.Notify();
    });

    done.Wait();
    return std::move(mover).get();
  }

  template <typename F> void Add(F&& f) {
    size_t start = next_index_.fetch_add(1, std::memory_order_relaxed) % worker_size_;
    Worker& main_w = workers_[start];
    while (true) {
      EventCount::Key key = main_w.push_ec.prepareWait();
      if (AddAnyWorker(start, std::forward<F>(f))) {
        break;
      }

      main_w.push_ec.wait(key.epoch());
    }
  }

  // Runs f on a worker pinned by "index". index does not have to be in range.
  template <typename F> void Add(size_t index, F&& f) {
    size_t start = index % worker_size_;
    Worker& main_w = workers_[start];
    while (true) {
      EventCount::Key key = main_w.push_ec.prepareWait();
      if (main_w.q->try_enqueue(std::forward<F>(f))) {
        main_w.pull_ec.notify();
        return;
      }

      main_w.push_ec.wait(key.epoch());
    }
  }

  void Shutdown();

 private:
  size_t wrapped_idx(size_t i) { return i < worker_size_ ? i : i - worker_size_; }

  template <typename F> bool AddAnyWorker(size_t start, F&& f) {
    for (size_t i = 0; i < worker_size_; ++i) {
      auto& w = workers_[wrapped_idx(start + i)];
      if (w.q->try_enqueue(std::forward<F>(f))) {
        w.pull_ec.notify();
        return true;
      }
    }
    return false;
  }

  void WorkerFunction(unsigned index);

  using FuncQ = base::mpmc_bounded_queue<Func>;

  struct Worker {
    pthread_t tid;
    std::unique_ptr<FuncQ> q;
    EventCount push_ec, pull_ec;
  };

  std::unique_ptr<Worker[]> workers_;
  size_t worker_size_;
  // base::mpmc_bounded_queue<Func> q_;

  std::atomic_ulong next_index_{0};
  std::atomic_bool is_closed_{false};
};

}  // namespace fibers_ext
}  // namespace util
