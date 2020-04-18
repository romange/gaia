// Copyright 2020, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/uring/proactor_pool.h"

#include "base/logging.h"
#include "base/pthread_utils.h"

DEFINE_uint32(proactor_threads, 0, "Number of io threads in the pool");

using namespace std;

namespace util {
namespace uring {

ProactorPool::ProactorPool(std::size_t pool_size) {
  if (pool_size == 0) {
    pool_size = FLAGS_proactor_threads > 0 ? FLAGS_proactor_threads
                                           : thread::hardware_concurrency();
  }
  pool_size_ = pool_size;
  proactor_.reset(new Proactor[pool_size]);
}

ProactorPool::~ProactorPool() {
  Stop();
}

void ProactorPool::CheckRunningState() {
  CHECK_EQ(RUN, state_);
}

void ProactorPool::Run(uint32_t ring_depth) {
  CHECK_EQ(STOPPED, state_);

  char buf[32];

  for (size_t i = 0; i < pool_size_; ++i) {
    snprintf(buf, sizeof(buf), "Proactor%lu", i);
    auto cb = [ptr = &proactor_[i], ring_depth]() { ptr->Run(ring_depth); };
    pthread_t tid = base::StartThread(buf, cb);
    cpu_set_t cps;
    CPU_ZERO(&cps);
    CPU_SET(i % thread::hardware_concurrency(), &cps);

    int rc = pthread_setaffinity_np(tid, sizeof(cpu_set_t), &cps);
    LOG_IF(WARNING, rc) << "Error calling pthread_setaffinity_np: "
                        << strerror(rc) << "\n";
  }

  AwaitOnAll([](Proactor*) {});

  LOG(INFO) << "Running " << pool_size_ << " io threads";
  state_ = RUN;
}

void ProactorPool::Stop() {
  if (state_ == STOPPED)
    return;

  for (size_t i = 0; i < pool_size_; ++i) {
    proactor_[i].Stop();
  }

  VLOG(1) << "Proactors have been stopped";

  for (size_t i = 0; i < pool_size_; ++i) {
    pthread_join(proactor_[i].thread_id(), nullptr);
    VLOG(2) << "Thread " << i << " has joined";
  }
  state_ = STOPPED;
}

}  // namespace uring
}  // namespace util
