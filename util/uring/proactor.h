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

class UringFiberAlgo;

class Proactor {
  Proactor(const Proactor&) = delete;
  void operator=(const Proactor&) = delete;

 public:
  explicit Proactor(unsigned ring_depth = 512);
  ~Proactor();

  template <typename Func> void Async(Func&& f);

  bool InProactorThread() const {
    return pthread_self() == thread_id_;
  }

  void Stop() {
    Async([this] { has_finished_ = true; });
  }

  // Runs the poll-loop. Stalls the calling thread which will become the "Proactor" thread.
  void Run();

  /**
   * The following methods can only run from Proactor::Run thread,i.e when
   * InProactorThread is true. You can call them from other threads by enqueuing via Async.
   */
  // TODO: to handle overflow use-cases.
  io_uring_sqe* GetSubmitEntry() {
    return io_uring_get_sqe(&ring_);
  }

  FdEvent* GetFdEvent(int fd);

 private:
  enum { WAIT_SECTION_STATE = 1UL << 31 };

  void WakeRing();

  void WakeupIfNeeded() {
    auto current = tq_seq_.fetch_add(1, std::memory_order_relaxed);
    if (current == WAIT_SECTION_STATE) {
      WakeRing();
    }
  }

  void DispatchCompletions(io_uring_cqe* cqes, unsigned count);

  template <typename Func> bool EmplaceTaskQueue(Func&& f) {
    if (task_queue_.try_enqueue(std::forward<Func>(f))) {
      WakeupIfNeeded();

      return true;
    }
    return false;
  }

  io_uring ring_;

  pthread_t thread_id_;

  int wake_fd_;
  bool has_finished_ = false;

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
  friend class UringFiberAlgo;
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
