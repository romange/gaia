// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "util/sq_threadpool.h"

#include <thread>
#include "base/logging.h"

namespace util {

using namespace boost;
using namespace std;

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
