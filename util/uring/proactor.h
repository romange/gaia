// Copyright 2020, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <liburing.h>
#include <pthread.h>

#include <boost/fiber/fiber.hpp>
#include <functional>

#include "base/function2.hpp"
#include "base/mpmc_bounded_queue.h"
#include "util/fibers/event_count.h"
#include "util/fibers/fibers_ext.h"
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

  bool InMyThread() const {
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
   * If cb is nullptr then no callback will be called upon completion of the request and payload
   * argument is ignored.
   */
  SubmitEntry GetSubmitEntry(CbType cb, int64_t payload);

  static bool IsProactorThread() {
    return tl_info_.is_proactor_thread;
  }

  bool HasFastPoll() const { return fast_poll_f_; }


  /**
   *  Message passing functions.
   * */

  //! Fire and forget - does not wait for the function to be called.
  //! Might block the calling fiber if the queue is full.
  template <typename Func> void Async(Func&& f);

  template <typename Func, typename... Args> void AsyncFiber(Func&& f, Args&&... args) {
    // Ideally we want to forward args into lambda but it's too complicated before C++20.
    // So I just copy them into capture.
    // We forward captured variables so we need lambda to be mutable.
    Async([f = std::forward<Func>(f), args...]() mutable {
      ::boost::fibers::fiber(std::forward<Func>(f), std::forward<Args>(args)...).detach();
    });
  }

  template <typename Func> auto Await(Func&& f) -> decltype(f());

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
  uint8_t fast_poll_f_ : 1;
  uint8_t reseved_f_   : 7;

  // We use fu2 function to allow moveable semantics.
  using Tasklet =
      fu2::function_base<true /*owns*/, false /*non-copyable*/, fu2::capacity_default,
                         false /* non-throwing*/, false /* strong exceptions guarantees*/, void()>;
  static_assert(sizeof(Tasklet) == 32, "");

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
    int32_t opcode = -1;  // For debugging. TODO: to remove later.
  };
  static_assert(sizeof(CompletionEntry) == 40, "");

  std::vector<CompletionEntry> centries_;
  int32_t next_free_ = -1;

  struct TLInfo {
    bool is_proactor_thread = false;
  };
  static thread_local TLInfo tl_info_;
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

template <typename Func> auto Proactor::Await(Func&& f) -> decltype(f()) {
  if (InMyThread()) {
    return f();
  }
  if (IsProactorThread()) {
    // TODO:
  }

  fibers_ext::Done done;
  using ResultType = decltype(f());
  detail::ResultMover<ResultType> mover;

  // Store done-ptr by value to increase the refcount while lambda is running.
  Async([&mover, f = std::forward<Func>(f), done]() mutable {
    mover.Apply(f);
    done.Notify();
  });

  done.Wait();
  return std::move(mover).get();
}

}  // namespace uring
}  // namespace util
