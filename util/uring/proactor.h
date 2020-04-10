// Copyright 2020, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <liburing.h>
#include <pthread.h>

#include <functional>

#include "base/mpmc_bounded_queue.h"
#include "util/fibers/event_count.h"

namespace util {

namespace uring {

class Proactor {
 public:
  explicit Proactor(unsigned ring_depth = 512);
  ~Proactor();

  // Runs the poll-loop. Stalls the calling thread.
  void Run();

  template <typename Func> void Async(Func&& f);

  bool InProactorThread() const {
    return pthread_self() == thread_id_;
  }

  void Stop() {
    Async([this] { has_finished_ = true; });
  }

 private:
  void WakeIfNeeded();

  template <typename Func> bool EmplaceTaskQueue(Func&& f) {
    if (!task_queue_.try_enqueue(std::forward<Func>(f)))
      return false;

    WakeIfNeeded();
    return true;
  }

  bool has_finished_ = false;
  io_uring ring_;

  pthread_t thread_id_;
  int wake_fd_;

  using CbFunc = std::function<void()>;
  using FuncQ = base::mpmc_bounded_queue<CbFunc>;
  using EventCount = fibers_ext::EventCount;

  FuncQ task_queue_;
  std::atomic_uint32_t tq_seq_{0}, tq_wakeups_{0};
  EventCount task_queue_avail_;
};

template <typename Func> void Proactor::Async(Func&& f) {
  if (EmplaceTaskQueue(std::forward<Func>(f)))
    return;

  while (true) {
    EventCount::Key key = task_queue_avail_.prepareWait();

    if (EmplaceTaskQueue(std::forward<Func>(f))) {
      break;
    }
    task_queue_avail_.wait(key.epoch());
  }
}

}  // namespace uring

}  // namespace util
