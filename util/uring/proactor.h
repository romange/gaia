// Copyright 2020, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <liburing.h>
#include <pthread.h>

#include <boost/fiber/fiber.hpp>
#include <boost/intrusive/slist.hpp>
#include <functional>

#include "base/mpmc_bounded_queue.h"
#include "util/fibers/event_count.h"
#include "util/uring/submit_entry.h"

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

  template <typename Func, typename... Args> void AsyncFiber(Func&& f, Args&&... args) {
    // Ideally we want to forward args into lambda but it's too complicated before C++20.
    // So I just copy them into capture.
    // We forward captured variables so we need lambda to be mutable.
    Async([f = std::forward<Func>(f), args...]() mutable {
      ::boost::fibers::fiber(std::forward<Func>(f), std::forward<Args>(args)...).detach();
    });
  }

  bool InProactorThread() const {
    return pthread_self() == thread_id_;
  }

  void Stop() {
    Async([this] { has_finished_ = true; });
  }

  // Runs the poll-loop. Stalls the calling thread which will become the "Proactor" thread.
  void Run();

  using IoResult = int;

  // IoResult is the I/O result of the completion event.
  // int64_t is the payload supplied during event submission. See GetSubmitEntry below.
  using CbType = std::function<void(IoResult, int64_t, Proactor*)>;

  /**
   * The following methods can only run from Proactor::Run thread,i.e when
   * InProactorThread is true. You can call them from other threads by enqueing via Async.
   */
  // TODO: to handle SQE overflow use-cases.
  SubmitEntry GetSubmitEntry(CbType cb, int64_t payload);

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

  void RegrowCentries();

  io_uring ring_;

  pthread_t thread_id_;

  int wake_fd_;
  bool has_finished_ = false;

  using Tasklet = std::function<void()>;
  using FuncQ = base::mpmc_bounded_queue<Tasklet>;
  using EventCount = fibers_ext::EventCount;

  FuncQ task_queue_;
  std::atomic_uint32_t tq_seq_{0}, tq_wakeups_{0};
  EventCount task_queue_avail_, sqe_avail_;
  ::boost::fibers::context* main_loop_ctx_ = nullptr;

  friend class UringFiberAlgo;

  struct CompletionEntry {
    CbType cb;

    // serves for linked list management when unused. Also can store an additional payload
    // field when in flight.
    int32_t val = -1;
    int32_t opcode = -1;   // For debugging. TODO: to remove later.
  };
  static_assert(sizeof(CompletionEntry) == 40, "");

  std::vector<CompletionEntry> centries_;
  int32_t next_free_ = -1;
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
