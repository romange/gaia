// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/asio/io_context_pool.h"

#include <condition_variable>

#include <boost/asio/steady_timer.hpp>
#include <boost/fiber/condition_variable.hpp>
#include <boost/fiber/mutex.hpp>
#include <boost/fiber/scheduler.hpp>

#include "base/logging.h"
#include "base/pthread_utils.h"

using namespace boost;

namespace util {

thread_local size_t IoContextPool::context_indx_ = 0;

IoContextPool::IoContextPool(std::size_t pool_size) {
  if (pool_size == 0)
    pool_size = std::thread::hardware_concurrency();
  context_arr_.resize(pool_size);
  thread_arr_.resize(pool_size);
}

IoContextPool::~IoContextPool() {
  Stop();
}

void IoContextPool::WrapLoop(size_t index, fibers_ext::BlockingCounter* bc) {
  context_indx_ = index;

  auto& context = context_arr_[index];
  VLOG(1) << "Starting io thread " << index;

  context.StartLoop(bc);

  VLOG(1) << "Finished io thread " << index;
}

void IoContextPool::Run() {
  CHECK_EQ(STOPPED, state_);

  fibers_ext::BlockingCounter bc(thread_arr_.size());

  for (size_t i = 0; i < thread_arr_.size(); ++i) {
    thread_arr_[i].work.emplace(asio::make_work_guard(*context_arr_[i].context_ptr_));
    thread_arr_[i].tid = base::StartThread("IoPool", [this, i, &bc] {
      this->WrapLoop(i, &bc);
    });
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
    tinfo.work->reset();
  }

  for (size_t i = 0; i <thread_arr_.size(); ++i) {
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

}  // namespace util
