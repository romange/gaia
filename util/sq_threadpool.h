// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <memory>
#include <string>

#include <boost/fiber/future.hpp>

#include "strings/stringpiece.h"
#include "util/fiberqueue_threadpool.h"
#include "util/status.h"

namespace util {

class FileIOManager {
 public:
  typedef boost::fibers::future<StatusObject<size_t>> ReadResult;

  FileIOManager(unsigned num_threads, unsigned queue_size = 128)
      : sq_tp_(num_threads, queue_size) {}

  ReadResult Read(int fd, size_t offs, strings::MutableByteRange dest);

 private:
  FiberQueueThreadPool sq_tp_;
};

}  // namespace util
