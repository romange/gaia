// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <boost/fiber/condition_variable.hpp>

namespace util {
namespace fibers {

// Wrap canonical pattern for condition_variable + bool flag
class Done {
 public:
  Done() {}
  Done(const Done&) = delete;

  void operator=(const Done&) = delete;

  void wait() {
    std::unique_lock<::boost::fibers::mutex> lock(mutex_);
    cond_.wait(lock, [this]() { return ready_; });
  }

  void notify() {
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

}  // namespace fibers
}  // namespace util
