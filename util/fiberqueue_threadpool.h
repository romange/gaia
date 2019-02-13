// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <boost/fiber/buffered_channel.hpp>
#include "util/fibers_ext.h"

namespace util {

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

    auto op_st = Add([&, f = std::forward<Func>(f)]() mutable {
      mover.Apply(f);
      done.Notify();
    });
    VerifyChannelSt(op_st);

    done.Wait();
    return std::move(mover).get();
  }

  template <typename Func> void Async(Func&& f) {
    auto op_st = Add(std::forward<Func>(f));
    VerifyChannelSt(op_st);
  }

  template <typename F> boost::fibers::channel_op_status Add(F&& f) {
    return input_.push(std::forward<F>(f));
  }

  void Shutdown();

 private:
  static void VerifyChannelSt(boost::fibers::channel_op_status st);

  void WorkerFunction();

  std::vector<pthread_t> workers_;
  boost::fibers::buffered_channel<Func> input_;
};

}  // namespace util
