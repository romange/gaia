// Copyright 2020, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/uring/proactor.h"

#include <liburing.h>
#include <string.h>

#include <sys/eventfd.h>
#include <sys/poll.h>

#include "base/logging.h"
#include "base/macros.h"

#define URING_CHECK(x)                                                           \
  do {                                                                           \
    int __res_val = (x);                                                         \
    if (UNLIKELY(__res_val < 0)) {                                               \
      char buf[128];                                                             \
      char* str = strerror_r(-__res_val, buf, sizeof(buf));                      \
      LOG(FATAL) << "Error " << (-__res_val) << " evaluating '" #x "': " << str; \
    }                                                                            \
  } while (false)

namespace util {
namespace uring {

namespace {

class FdEvent;

using CbType = std::function<void(int32_t, io_uring*, FdEvent*)>;

class FdEvent {
  int fd_ = -1;
  CbType cb_;  // This lambda might hold auxillary data that is needed to run.

 public:
  explicit FdEvent(int fd) : fd_(fd) {
  }

  void Set(CbType cb) {
    cb_ = std::move(cb);
  }

  int fd() const {
    return fd_;
  }

  void Run(int res, io_uring* ring) {
    cb_(res, ring, this);
  }
};

}  // namespace

Proactor::Proactor() : task_queue_(128) {
  thread_id_ = pthread_self();

  io_uring_params params;
  memset(&params, 0, sizeof(params));
  URING_CHECK(io_uring_queue_init_params(4096, &ring_, &params));

  if ((params.features & IORING_FEAT_FAST_POLL) == 0) {
    LOG_FIRST_N(INFO, 1) << "IORING_FEAT_FAST_POLL feature is not present in the kernel";
  }

  if (params.features & IORING_FEAT_SINGLE_MMAP) {
    size_t sz = ring_.sq.ring_sz + params.sq_entries * sizeof(struct io_uring_sqe);
    LOG_FIRST_N(INFO, 1) << "IORing with " << params.sq_entries << " allocated " << sz << " bytes";
  }

  wake_fd_ = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
  CHECK_GT(wake_fd_, 0);

  struct io_uring_sqe *sqe = io_uring_get_sqe(&ring_);
  CHECK_NOTNULL(sqe);

	io_uring_prep_poll_add(sqe, wake_fd_, POLLIN);
	sqe->user_data = 1;
}

Proactor::~Proactor() {
  io_uring_queue_exit(&ring_);
  close(wake_fd_);
}

void Proactor::Run() {
  io_uring_cqe* cqe = nullptr;
  constexpr size_t kBatchSize = 32;
  struct io_uring_cqe* cqes[kBatchSize];
  unsigned cqe_count = 0;
  CbFunc task;

  while (true) {
    // tell kernel we have put a sqe on the submission ring.
    // Might return negative -errno.
    int num_submitted = io_uring_submit(&ring_);
    URING_CHECK(num_submitted);

    uint32_t tq_seq = tq_seq_.load(std::memory_order_relaxed);

    do {
      DCHECK_EQ(0, tq_seq & WAIT_SECTION_MASK);

      while (task_queue_.try_dequeue(task)) {
        task_queue_avail_.notify();
        task();
      }
    } while (tq_seq_.compare_exchange_weak(tq_seq, tq_seq | WAIT_SECTION_MASK));

    // wait for new cqe to become available.
    int res = io_uring_wait_cqe_nr(&ring_, &cqe, 1);  // res must be <= 0.
    tq_seq_.store(0, std::memory_order_release);

    if (res < 0) {
      if (-res == EINTR)
        break;
      URING_CHECK(res);
    }
    CHECK_EQ(0, res);

    do {
      // check how many cqes are in the cq-queue, and put these cqes in an array.
      cqe_count = io_uring_peek_batch_cqe(&ring_, cqes, kBatchSize);
      DVLOG(1) << "io_uring_peek_batch_cqe returned " << cqe_count << " completions";

      io_uring_cqe cqe;
      for (unsigned i = 0; i < cqe_count; ++i) {
        cqe = *cqes[i];
        io_uring_cq_advance(&ring_, 1);  // Once we copied the data we can mark the cqe consumed.

        if (cqe.user_data <= 1) {

        } else {
          FdEvent* event = reinterpret_cast<FdEvent*>(io_uring_cqe_get_data(&cqe));
          event->Run(res, &ring_);
        }
      }
    } while (cqe_count == kBatchSize);
  }
}

void Proactor::WakeIfNeeded() {
  auto prev = tq_seq_.fetch_add(1, std::memory_order_acq_rel);
  if (prev & WAIT_SECTION_MASK) {
    uint64_t val = 1;

    CHECK_EQ(8, write(wake_fd_, &val, sizeof(uint64_t)));
  }
}

}  // namespace uring

}  // namespace util
