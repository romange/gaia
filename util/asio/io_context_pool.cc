// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/asio/io_context_pool.h"

#include <sched.h>
#include <boost/asio/steady_timer.hpp>
#include <boost/fiber/mutex.hpp>
#include <boost/fiber/scheduler.hpp>

#include "base/logging.h"
#include "base/pthread_utils.h"

using namespace boost;
using std::thread;

DEFINE_uint32(io_context_threads, 0, "Number of io threads in the pool");

namespace util {

thread_local size_t IoContextPool::context_indx_ = 0;

IoContextPool::IoContextPool(size_t pool_size, std::vector<int> cpus) {
  if (pool_size == 0) {
    pool_size =
        FLAGS_io_context_threads > 0 ? FLAGS_io_context_threads : thread::hardware_concurrency();
  }
  if (cpus.empty()) {
    for (size_t i = 0; i < pool_size; ++i)
      cpus.push_back(i);
  }
  CHECK_EQ(pool_size, cpus.size());
  cpu_idx_arr_ = std::move(cpus);
  context_arr_.resize(pool_size);
  thread_arr_.resize(pool_size);
}

IoContextPool::~IoContextPool() { Stop(); }

void IoContextPool::WrapLoop(size_t index, fibers_ext::BlockingCounter* bc) {
  context_indx_ = index;

  auto& context = context_arr_[index];
  VLOG(1) << "Starting io thread " << index;

  context.StartLoop(bc);

  VLOG(1) << "Finished io thread " << index;
}

void IoContextPool::CheckRunningState() {
  CHECK_EQ(RUN, state_);
}

void IoContextPool::Run() {
  CHECK_EQ(STOPPED, state_);

  fibers_ext::BlockingCounter bc(thread_arr_.size());
  char buf[32];

  for (size_t i = 0; i < thread_arr_.size(); ++i) {
    thread_arr_[i].work.emplace(asio::make_work_guard(*context_arr_[i].context_ptr_));
    snprintf(buf, sizeof(buf), "IoPool%lu", i);
    thread_arr_[i].tid =
        base::StartThread(buf, [this, i, bc]() mutable { this->WrapLoop(i, &bc); });
    cpu_set_t cps;
    CPU_ZERO(&cps);
    CPU_SET(cpu_idx_arr_[i] % thread::hardware_concurrency(), &cps);

    int rc = pthread_setaffinity_np(thread_arr_[i].tid, sizeof(cpu_set_t), &cps);
    LOG_IF(WARNING, rc) << "Error calling pthread_setaffinity_np: " << strerror(rc) << "\n";
  }

  // We can not use Await() here yet because StartLoop might not run yet and its implementation
  // assumes internally that the first posted handler is issued from the StartLoop.
  // Therefore we use BlockingCounter to wait for all the IO loops to start running.
  bc.Wait();

  LOG(INFO) << "Running " << thread_arr_.size() << " io threads";
  state_ = RUN;
}

void IoContextPool::Stop() {
  if (state_ == STOPPED)
    return;

  for (size_t i = 0; i < context_arr_.size(); ++i) {
    context_arr_[i].Stop();
  }

  for (TInfo& tinfo : thread_arr_) {
    tinfo.work.reset();
  }
  VLOG(1) << "Asio Contexts has been stopped";

  for (size_t i = 0; i < thread_arr_.size(); ++i) {
    pthread_join(thread_arr_[i].tid, nullptr);
    VLOG(2) << "Thread " << i << " has joined";
  }
  state_ = STOPPED;
}

IoContext& IoContextPool::GetNextContext() {
  // Use a round-robin scheme to choose the next io_context to use.
  DCHECK_LT(next_io_context_, context_arr_.size());
  uint32_t index = next_io_context_.load();
  IoContext& io_context = context_arr_[index++];

  // Not-perfect round-robind since this function is non-transactional but it's valid.
  if (index == context_arr_.size())
    next_io_context_ = 0;
  else
    next_io_context_ = index;
  return io_context;
}

IoContext* IoContextPool::GetThisContext() {
  CHECK_EQ(state_, RUN);
  pthread_t self = pthread_self();

  for (size_t i = 0; i < thread_arr_.size(); ++i) {
    if (thread_arr_[i].tid == self) {
      return &context_arr_[i];
    }
  }
  return nullptr;
}

}  // namespace util
