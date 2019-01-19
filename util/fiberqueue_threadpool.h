// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <boost/fiber/buffered_channel.hpp>

namespace util {

// This thread pool has a global fiber-friendly queue for incoming tasks.
class FiberQueueThreadPool {
 public:
  // I must use folly::Function because std & boost functions do not wrap MoveableOnly lambdas.
  typedef std::function<void()> Func;

  explicit FiberQueueThreadPool(unsigned num_threads, unsigned queue_size = 128);
  ~FiberQueueThreadPool();

  template <typename F> boost::fibers::channel_op_status Add(F&& f) {
    return input_.push(std::forward<F>(f));
  }

  void Shutdown();

 private:
  void WorkerFunction();

  std::vector<pthread_t> workers_;
  boost::fibers::buffered_channel<Func> input_;
};

}  // namespace util

