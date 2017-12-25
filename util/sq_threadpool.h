// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <memory>
#include <string>
#include "strings/stringpiece.h"
#include "util/status.h"

#include <boost/fiber/buffered_channel.hpp>
#include <boost/fiber/future.hpp>
#include <boost/thread.hpp>

namespace util {

// This thread pool has a global fiber-friendly queue for incoming tasks.
class SingleQueueThreadPool {
 public:
  // I must use folly::Function because std & boost functions do not wrap MoveableOnly lambdas.
  typedef std::function<void()> Func;
  
  explicit SingleQueueThreadPool(unsigned num_threads, unsigned queue_size = 128); 

  ~SingleQueueThreadPool();

  boost::fibers::channel_op_status Add(Func&& f) {
    return input_.push(std::move(f));
  }
  
  void Shutdown();

 private:
  void WorkerFunction();

  std::vector<boost::thread> workers_;

  boost::fibers::buffered_channel<Func> input_;
};


class FileIOManager {
 public:
  typedef boost::fibers::future<StatusObject<size_t>> ReadResult;

  FileIOManager(unsigned num_threads, unsigned queue_size = 128) 
    : sq_tp_(num_threads, queue_size) {}

  ReadResult Read(int fd, size_t offs, strings::MutableByteRange dest);

 private:
  SingleQueueThreadPool sq_tp_;
};

}  // namespace util
