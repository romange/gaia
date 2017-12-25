// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "util/sq_threadpool.h"
#include "base/logging.h"

namespace util {

using namespace boost;

namespace {

typedef StatusObject<size_t> ReadStatus;

Status StatusFileError() {
  char buf[1024];
  char* result = strerror_r(errno, buf, sizeof(buf));

  return Status(StatusCode::IO_ERROR, result);
}

ReadStatus ReadNoInt(int fd, uint8_t* buffer, size_t length, size_t offset) {
  size_t left_to_read = length;
  uint8_t* curr_buf = buffer;

  while (left_to_read > 0) {
    ssize_t read = pread(fd, curr_buf, left_to_read, offset);
    if (read <= 0) {
      return read == 0 ? ReadStatus(length - left_to_read) : StatusFileError();
    }

    curr_buf += read;
    offset += read;
    left_to_read -= read;
  }
  return length;
}

}  // namespace

SingleQueueThreadPool::SingleQueueThreadPool(unsigned num_threads, unsigned queue_size) 
    : input_(queue_size) {
  thread::attributes attrs;
  
  if (num_threads == 0) {
    num_threads = thread::hardware_concurrency();
  }
  attrs.set_stack_size(1 << 16);

  workers_.reserve(num_threads);
  for (unsigned i = 0; i < num_threads; ++i) {
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
  for (auto& w : workers_) {
    w.join();
  }
}


void SingleQueueThreadPool::WorkerFunction() {
  while (true) {
    Func f;
    fibers::channel_op_status st = input_.pop(f);
    if (st == fibers::channel_op_status::closed)
      break;
    CHECK(st == fibers::channel_op_status::success) << int(st);
    try {
      f();
    } catch(std::exception& e) {
      std::exception_ptr p = std::current_exception();
      LOG(FATAL) << "Exception " << e.what();
    }
  }
}


auto FileIOManager::Read(int fd, size_t offs, strings::MutableByteRange dest) -> ReadResult {
  auto pr = std::make_shared<fibers::promise<ReadStatus>>();
  
  fibers::channel_op_status st = sq_tp_.Add([fd, offs, dest, pr] () mutable {
    ReadStatus st = ReadNoInt(fd, dest.data(), dest.size(), offs);
    pr->set_value(st);
  });

  if (st != fibers::channel_op_status::success) {
    switch (st) {
      case fibers::channel_op_status::closed:
        pr->set_value(Status(StatusCode::IO_ERROR, "Channel closed"));
      break;
      default:
        pr->set_value(Status("Unknown error!"));
    }
  }
  return pr->get_future();
}

}  // namespace util
