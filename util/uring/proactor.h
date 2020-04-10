// Copyright 2020, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <liburing.h>
#include <pthread.h>

#include <boost/intrusive/slist.hpp>
#include <functional>

#include "base/mpmc_bounded_queue.h"
#include "util/fibers/event_count.h"
#include "util/uring/fdevent.h"

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

  // TODO: to handle ooverflow use-cases.
  io_uring_sqe* GetSubmitEntry() {
    return io_uring_get_sqe(&ring_);
  }

  FdEvent* GetFdEvent(int fd);

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

  using ListType = boost::intrusive::slist<FdEvent, FdEvent::member_hook_t,
                                           boost::intrusive::constant_time_size<false>,
                                           boost::intrusive::cache_last<false>>;

  ListType event_fd_list_;
  friend class FdEvent;
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

inline void FdEvent::AddPollin(Proactor* proactor) {
  io_uring_sqe* sqe = proactor->GetSubmitEntry();

  io_uring_prep_poll_add(sqe, handle(), POLLIN);
  io_uring_sqe_set_data(sqe, this);
}

}  // namespace uring
}  // namespace util
