// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <memory>
#include <string>
#include "strings/stringpiece.h"
#include <boost/fiber/buffered_channel.hpp>
#include <boost/thread.hpp>

namespace util {

// This thread pool has a global fiber-friendly queue for incoming tasks.
class SingleQueueThreadPool {
 public:
  explicit SingleQueueThreadPool(unsigned num_threads, unsigned queue_size = 128); 

  ~SingleQueueThreadPool();

  boost::fibers::channel_op_status Add(std::function<void()>&& f) {
    return input_.push(std::move(f));
  }
  
  void Shutdown();

 private:
  void WorkerFunction();

  std::vector<boost::thread> workers_;

  boost::fibers::buffered_channel<std::function<void()>> input_;
};


}  // namespace util
