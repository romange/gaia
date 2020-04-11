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
  for (int i = 0; head != last; head++, i++)
    cqes[i] = ring.cq.cqes[head & mask];

  return count;
}

void DisposeFdEvent(FdEvent* ptr) {
  delete ptr;
}

}  // namespace

Proactor::Proactor(unsigned ring_depth) : task_queue_(128) {
  io_uring_params params;
  memset(&params, 0, sizeof(params));
  URING_CHECK(io_uring_queue_init_params(ring_depth, &ring_, &params));

  if ((params.features & IORING_FEAT_FAST_POLL) == 0) {
    LOG_FIRST_N(INFO, 1) << "IORING_FEAT_FAST_POLL feature is not present in the kernel";
  }

  if (params.features & IORING_FEAT_SINGLE_MMAP) {
    size_t sz = ring_.sq.ring_sz + params.sq_entries * sizeof(struct io_uring_sqe);
    LOG_FIRST_N(INFO, 1) << "IORing with " << params.sq_entries << " allocated " << sz << " bytes";
  }

  wake_fd_ = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
  CHECK_GT(wake_fd_, 0);

  struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
  CHECK_NOTNULL(sqe);

  io_uring_prep_poll_add(sqe, wake_fd_, POLLIN);
  sqe->user_data = 1;

  volatile ctx::fiber
      dummy;  // For some weird reason I need this to pull boost::context into linkage.
}

Proactor::~Proactor() {
  io_uring_queue_exit(&ring_);
  close(wake_fd_);
  event_fd_list_.clear_and_dispose(&DisposeFdEvent);
}

FdEvent* Proactor::GetFdEvent(int fd) {
  FdEvent* res = new FdEvent(fd);
  event_fd_list_.push_front(*res);

  return res;
}

void Proactor::Run() {
  LOG(INFO) << "Proactor::Run";

  thread_id_ = pthread_self();

  sigset_t mask;
  sigfillset(&mask);
  CHECK_EQ(0, pthread_sigmask(SIG_BLOCK, &mask, NULL));

  fibers::context* main_loop_ctx = fibers::context::active();

  fibers::scheduler* sched = main_loop_ctx->get_scheduler();

  UringFiberAlgo* scheduler = new UringFiberAlgo(this);
  sched->set_algo(scheduler);
  this_fiber::properties<UringFiberProps>().set_name("ioloop");


  constexpr size_t kBatchSize = 32;
  struct io_uring_cqe cqes[kBatchSize];
  uint32_t tq_seq = 0;
  uint32_t num_stalls = 0, empty_loops = 0;
  CbFunc task;

  while (true) {
    // tell kernel we have put a sqe on the submission ring.
    // Might return negative -errno.
    int num_submitted = io_uring_submit(&ring_);
    URING_CHECK(num_submitted);

    tq_seq = tq_seq_.load(std::memory_order_acquire);
    unsigned num_task_runs = 0;
    while (task_queue_.try_dequeue(task)) {
      ++num_task_runs;
      task();
    }

    if (num_task_runs) {
      // Should we put it inside the loop? It might improve the latency.
      task_queue_avail_.notifyAll();
    }

    uint32_t cqe_count = IoRingPeek(ring_, cqes, kBatchSize);
    if (cqe_count) {
      // Once we copied the data we can mark the cqe consumed.
      io_uring_cq_advance(&ring_, cqe_count);
      DVLOG(2) << "Fetched " << cqe_count << " cqes";

      DispatchCompletions(cqes, cqe_count);
      continue;
    }

    if (sched->has_ready_fibers()) {
      // Suspend this fiber until others will run and get blocked.
      // Eventually UringFiberAlgo will resume back this fiber in suspend_until function.
      sched->suspend();
      continue;
    }

    empty_loops += (num_task_runs == 0);

    /**
     * If tq_seq_ has changed since it was cached into tq_seq, then EmplaceTaskQueue succeeded
     * and we might have more tasks to execute - lets run the loop again.
     * Otherwise, set tq_seq_ to WAIT_SECTION, hinting that we are going to stall now.
     * Other threads will need to wake-up the ring (see WakeRing()) but only the they will
     * actually syscall only once.
     */
    if (tq_seq_.compare_exchange_weak(tq_seq, WAIT_SECTION_STATE, std::memory_order_relaxed)) {
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
}

void Proactor::WakeRing() {
  tq_wakeups_.fetch_add(1, std::memory_order_relaxed);
  uint64_t val = 1;

  CHECK_EQ(8, write(wake_fd_, &val, sizeof(uint64_t)));
}

void Proactor::DispatchCompletions(io_uring_cqe* cqes, unsigned count) {
  for (unsigned i = 0; i < count; ++i) {
    auto& cqe = cqes[i];

    // I leave here 1024 codes with predefined meanings.
    if (cqe.user_data == 1) {
      struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
      CHECK_EQ(8, read(wake_fd_, &cqe.user_data, 8));
      io_uring_prep_poll_add(sqe, wake_fd_, POLLIN);
      sqe->user_data = 1;
    } else if (cqe.user_data == 2) {
      // ....
    } else if (cqe.user_data > 1024) {  // our heap range surely starts higher than 1k.
      FdEvent* event = reinterpret_cast<FdEvent*>(io_uring_cqe_get_data(&cqe));
      event->Run(cqe.res, this);
    }
  }
}

void FdEvent::DisarmAndDiscard(Proactor* proactor) {
  auto it = proactor->event_fd_list_.iterator_to(*this);
  CHECK(it != proactor->event_fd_list_.end());

  cb_ = nullptr;
  proactor->event_fd_list_.erase_and_dispose(it, &DisposeFdEvent);
}

}  // namespace uring
}  // namespace util
