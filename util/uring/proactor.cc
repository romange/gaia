// Copyright 2020, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/uring/proactor.h"

#include <liburing.h>
#include <string.h>
#include <sys/eventfd.h>
#include <sys/poll.h>

#include <boost/fiber/operations.hpp>
#include <boost/fiber/scheduler.hpp>

#include "base/logging.h"
#include "base/macros.h"
#include "util/uring/uring_fiber_algo.h"

#define URING_CHECK(x)                                                           \
  do {                                                                           \
    int __res_val = (x);                                                         \
    if (UNLIKELY(__res_val < 0)) {                                               \
      char buf[128];                                                             \
      char* str = strerror_r(-__res_val, buf, sizeof(buf));                      \
      LOG(FATAL) << "Error " << (-__res_val) << " evaluating '" #x "': " << str; \
    }                                                                            \
  } while (false)

# ifndef __NR_io_uring_enter
#  define __NR_io_uring_enter		426
# endif

using namespace boost;
namespace ctx = boost::context;

namespace util {
namespace uring {

namespace {

inline int sys_io_uring_enter(int fd, unsigned to_submit, unsigned min_complete, unsigned flags,
                              sigset_t* sig) {
  return syscall(__NR_io_uring_enter, fd, to_submit, min_complete, flags, sig, _NSIG / 8);
}

inline void wait_for_cqe(io_uring* ring, sigset_t* sig = NULL) {
  // res must be 0 or -1.
  int res = sys_io_uring_enter(ring->ring_fd, 0, 1, IORING_ENTER_GETEVENTS, sig);
  if (res == 0 || errno == EINTR)
    return;
  DCHECK_EQ(-1, res);
  res = errno;

  LOG(FATAL) << "Error " << (res) << " evaluating sys_io_uring_enter: " << strerror(res);
}

inline unsigned CQReadyCount(const io_uring& ring) {
  return io_uring_smp_load_acquire(ring.cq.ktail) - *ring.cq.khead;
}

unsigned IoRingPeek(const io_uring& ring, io_uring_cqe* cqes, unsigned count) {
  unsigned ready = CQReadyCount(ring);
  if (!ready)
    return 0;

  count = count > ready ? ready : count;
  unsigned head = *ring.cq.khead;
  unsigned mask = *ring.cq.kring_mask;
  unsigned last = head + count;
  for (int i = 0; head != last; head++, i++) {
    cqes[i] = ring.cq.cqes[head & mask];
  }
  return count;
}

constexpr uint64_t kIgnoreIndex = 0;
constexpr uint64_t kWakeIndex = 1;
constexpr uint64_t kUserDataCbIndex = 1024;

}  // namespace


thread_local Proactor::TLInfo Proactor::tl_info_;

Proactor::Proactor(unsigned ring_depth) : task_queue_(128) {
  CHECK_GE(ring_depth, 8);

  io_uring_params params;
  memset(&params, 0, sizeof(params));
  URING_CHECK(io_uring_queue_init_params(ring_depth, &ring_, &params));

  fast_poll_f_ = (params.features & IORING_FEAT_FAST_POLL) != 0;
  if (!fast_poll_f_) {
    LOG_FIRST_N(INFO, 1) << "IORING_FEAT_FAST_POLL feature is not present in the kernel";
  }

  if (0 == (params.features & IORING_FEAT_NODROP)) {
    LOG_FIRST_N(INFO, 1) << "IORING_FEAT_NODROP feature is not present in the kernel";
  }

  if (params.features & IORING_FEAT_SINGLE_MMAP) {
    size_t sz = ring_.sq.ring_sz + params.sq_entries * sizeof(struct io_uring_sqe);
    LOG_FIRST_N(INFO, 1) << "IORing with " << params.sq_entries << " allocated " << sz
                         << " bytes, cq_entries is " << *ring_.cq.kring_entries;
  }
  CHECK_EQ(ring_depth, params.sq_entries);  // Sanity.

  wake_fd_ = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
  CHECK_GT(wake_fd_, 0);

  struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
  CHECK_NOTNULL(sqe);

  io_uring_prep_poll_add(sqe, wake_fd_, POLLIN);
  sqe->user_data = kWakeIndex;

  volatile ctx::fiber
      dummy;  // For some weird reason I need this to pull boost::context into linkage.
  centries_.resize(params.sq_entries);
  next_free_ = 0;
  for (size_t i = 0; i < centries_.size() - 1; ++i) {
    centries_[i].val = i + 1;
  }
}

Proactor::~Proactor() {
  io_uring_queue_exit(&ring_);
  close(wake_fd_);
}

void Proactor::Run() {
  LOG(INFO) << "Proactor::Run";

  thread_id_ = pthread_self();

  // Should we do it here?
  sigset_t mask;
  sigfillset(&mask);
  CHECK_EQ(0, pthread_sigmask(SIG_BLOCK, &mask, NULL));

  main_loop_ctx_ = fibers::context::active();
  fibers::scheduler* sched = main_loop_ctx_->get_scheduler();

  UringFiberAlgo* scheduler = new UringFiberAlgo(this);
  sched->set_algo(scheduler);
  this_fiber::properties<UringFiberProps>().set_name("ioloop");

  tl_info_.is_proactor_thread = true;

  constexpr size_t kBatchSize = 32;
  struct io_uring_cqe cqes[kBatchSize];
  uint32_t tq_seq = 0;
  uint32_t num_stalls = 0, empty_loops = 0;
  unsigned num_task_runs = 0;
  Tasklet task;
  int64_t inflight_requests = 0;

  while (true) {
    // tell kernel we have put a sqe on the submission ring.
    // Might return negative -errno.
    // The sqe buffer is [sqe_head, sqe_tail). io_uring_submit advances sqe_head to sqe_tail
    // by copying the sqe buffer contents into kernel buffer.
    // In case of an retryable error (EBUSY), we need to restore sqe_head to its previous value.
    auto sq_head = ring_.sq.sqe_head;
    bool sq_busy = false;
    int num_submitted = io_uring_submit(&ring_);

    if (num_submitted >= 0) {
      inflight_requests += num_submitted;
    } else if (num_submitted == -EBUSY) {
      VLOG(1) << "EBUSY " << num_submitted;
      num_submitted = 0;
      ring_.sq.sqe_head = sq_head;
      sq_busy = true;
    } else {
      URING_CHECK(num_submitted);
    }

    num_task_runs = 0;

    tq_seq = tq_seq_.load(std::memory_order_acquire);

    // This should handle wait-free and "submit-free" short CPU tasks enqued
    // using Async/Await calls.
    while (task_queue_.try_dequeue(task)) {
      ++num_task_runs;
      task();
    }

    if (num_task_runs) {
      // Should we put 'notify' inside the loop? It might improve the latency.
      task_queue_avail_.notifyAll();

      DCHECK(sq_busy || 0 == io_uring_sq_ready(&ring_));
    }

    uint32_t cqe_count = IoRingPeek(ring_, cqes, kBatchSize);
    if (cqe_count) {
      // Once we copied the data we can mark the cqe consumed.
      io_uring_cq_advance(&ring_, cqe_count);
      inflight_requests -= cqe_count;
      DVLOG(2) << "Fetched " << cqe_count << " cqes, inflight: " << inflight_requests;
      sqe_avail_.notifyAll();

      DispatchCompletions(cqes, cqe_count);
    }

    if (sched->has_ready_fibers()) {
      // Suspend this fiber until others will run and get blocked.
      // Eventually UringFiberAlgo will resume back this fiber in suspend_until function.
      sched->suspend();
      continue;
    }

    if (cqe_count)
      continue;

    empty_loops += ((num_task_runs + num_submitted) == 0);

    /**
     * If tq_seq_ has changed since it was cached into tq_seq, then EmplaceTaskQueue succeeded
     * and we might have more tasks to execute - lets run the loop again.
     * Otherwise, set tq_seq_ to WAIT_SECTION, hinting that we are going to stall now.
     * Other threads will need to wake-up the ring (see WakeRing()) but only the they will
     * actually syscall only once.
     */
    if (tq_seq_.compare_exchange_weak(tq_seq, WAIT_SECTION_STATE, std::memory_order_acquire)) {
      if (has_finished_)
        break;

      wait_for_cqe(&ring_);
      tq_seq = 0;
      ++num_stalls;
      tq_seq_.store(0, std::memory_order_release);
    }
  }

  VLOG(1) << "wakeups/stalls/empty-loops: " << tq_wakeups_.load() << "/" << num_stalls << "/"
          << empty_loops;

  VLOG(1) << "centries size: " << centries_.size();
  centries_.clear();
}

void Proactor::WakeRing() {
  tq_wakeups_.fetch_add(1, std::memory_order_relaxed);
  uint64_t val = 1;

  CHECK_EQ(8, write(wake_fd_, &val, sizeof(uint64_t)));
}

void Proactor::DispatchCompletions(io_uring_cqe* cqes, unsigned count) {
  for (unsigned i = 0; i < count; ++i) {
    auto& cqe = cqes[i];
    if (cqe.user_data >= kUserDataCbIndex) {  // our heap range surely starts higher than 1k.
      size_t index = cqe.user_data - kUserDataCbIndex;
      DCHECK_LT(index, centries_.size());
      auto& e = centries_[index];
      DCHECK(e.cb) << index;

      CbType func;
      auto payload = e.val;
      func.swap(e.cb);

      e.val = next_free_;
      next_free_ = index;

      func(cqe.res, payload, this);
      continue;
    }

    if (cqe.user_data == kIgnoreIndex)
      continue;

    // I leave here 1024 codes with predefined meanings.
    if (cqe.user_data == kWakeIndex) {
      CHECK_EQ(8, read(wake_fd_, &cqe.user_data, 8));   // Pull the data

      // TODO: to move io_uring_get_sqe call from here to before we stall.
      struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
      io_uring_prep_poll_add(sqe, wake_fd_, POLLIN);    // And recharge
      sqe->user_data = 1;
    } else {
      LOG(ERROR) << "Unrecognized user_data " << cqe.user_data;
    }
  }
}

SubmitEntry Proactor::GetSubmitEntry(CbType cb, int64_t payload) {
  io_uring_sqe* res = io_uring_get_sqe(&ring_);
  if (res == NULL) {
    fibers::context* current = fibers::context::active();
    CHECK(current != main_loop_ctx_) << "SQE overflow in the main context";

    sqe_avail_.await([this] { return io_uring_sq_space_left(&ring_) > 0; });
    res = io_uring_get_sqe(&ring_);  // now we should have the space.
    CHECK(res);
  }

  if (cb) {
    if (next_free_ < 0) {
      RegrowCentries();
      DCHECK_GT(next_free_, 0);
    }

    res->user_data = next_free_ + kUserDataCbIndex;
    DCHECK_LT(next_free_, centries_.size());

    auto& e = centries_[next_free_];
    DCHECK(!e.cb);  // cb is undefined.
    DVLOG(1) << "GetSubmitEntry: index: " << next_free_ << ", socket: " << payload;

    next_free_ = e.val;
    e.cb = std::move(cb);
    e.val = payload;
    e.opcode = -1;
  } else {
    res->user_data = kIgnoreIndex;
  }

  return SubmitEntry{res};
}

void Proactor::RegrowCentries() {
  size_t prev = centries_.size();
  VLOG(1) << "RegrowCentries from " << prev << " to " << prev * 2;

  centries_.resize(prev * 2);  // grow by 2.
  next_free_ = prev;
  for (; prev < centries_.size() - 1; ++prev)
    centries_[prev].val = prev + 1;
}

}  // namespace uring
}  // namespace util
