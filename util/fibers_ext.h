// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <boost/fiber/condition_variable.hpp>

namespace util {
namespace fibers_ext {

// Wrap canonical pattern for condition_variable + bool flag
class Done {
 public:
  Done() {}
  Done(const Done&) = delete;

  void operator=(const Done&) = delete;

  void Wait() {
    std::unique_lock<::boost::fibers::mutex> lock(mutex_);
    cond_.wait(lock, [this]() { return ready_; });
  }

  void Notify() {
    mutex_.lock();
    ready_ = true;
    mutex_.unlock();
    cond_.notify_one();
  }
 private:
  ::boost::fibers::condition_variable   cond_;
  ::boost::fibers::mutex                mutex_;
  bool                                  ready_ = false;
};

class BlockingCounter {
 public:
  using mutex = ::boost::fibers::mutex;

  explicit BlockingCounter(unsigned count) : count_(count) {}

  void Add(unsigned delta) {
    std::lock_guard<mutex> g(mutex_);
    count_ += delta;
  }

  void Dec() {
    if (0 == count_)  // should not happen
      return;

    std::lock_guard<mutex> g(mutex_);
    --count_;
    if (count_ == 0)
      cond_.notify_one();
  }

  void Wait() {
    std::unique_lock<::boost::fibers::mutex> lock(mutex_);
    cond_.wait(lock, [this] { return count_ == 0; });
  }

 private:
  unsigned count_;

  ::boost::fibers::condition_variable   cond_;
  ::boost::fibers::mutex                mutex_;
};

}  // namespace fibers_ext
}  // namespace util
