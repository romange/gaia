// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <boost/fiber/buffered_channel.hpp>
#include "util/fibers/fibers_ext.h"

namespace util {
namespace fibers_ext {

// This thread pool has a global fiber-friendly queue for incoming tasks.
class FiberQueueThreadPool {
 public:
  typedef std::function<void()> Func;

  explicit FiberQueueThreadPool(unsigned num_threads = 0, unsigned queue_size = 128);
  ~FiberQueueThreadPool();

  template <typename Func> auto Await(Func&& f) -> decltype(f()) {
    fibers_ext::Done done;
    using ResultType = decltype(f());
    detail::ResultMover<ResultType> mover;

    Add([&, f = std::forward<Func>(f)]() mutable {
      mover.Apply(f);
      done.Notify();
    });

    done.Wait();
    return std::move(mover).get();
  }

  template <typename F> boost::fibers::channel_op_status Add(F&& f) {
    return input_.push(std::forward<F>(f));
  }

  void Shutdown();

 private:
  void WorkerFunction();

  std::vector<pthread_t> workers_;
  boost::fibers::buffered_channel<Func> input_;
};

}  // namespace fibers_ext
}  // namespace util
