// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "util/sq_threadpool.h"
#include "base/logging.h"

namespace util {

void Foo() {}

SingleQueueThreadPool::SingleQueueThreadPool(unsigned num_threads, unsigned queue_size) 
    : input_(queue_size) {
  boost::thread::attributes attrs;
  
  if (num_threads == 0) {
    num_threads = boost::thread::hardware_concurrency();
  }
  attrs.set_stack_size(1 << 14);

  workers_.reserve(num_threads);
  for (unsigned i = 0; i < num_threads; ++i) {
    // boost::thread t{};
    workers_.emplace_back(attrs, std::bind(&SingleQueueThreadPool::WorkerFunction, this));
  }
} 

SingleQueueThreadPool::~SingleQueueThreadPool() {
  if (!input_.is_closed()) {
    Shutdown();
  }
}

void SingleQueueThreadPool::Shutdown() {
  input_.close();
  for (auto& w : workers_)
    w.join();
}

}  // namespace util
