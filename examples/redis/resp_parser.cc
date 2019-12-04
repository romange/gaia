// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "examples/redis/resp_parser.h"
#include "base/logging.h"

namespace redis {

namespace {

uint8_t* ParseLine(uint8_t* buf, const uint8_t* end) {
  uint8_t* start = buf;

  while (start < end) {
    size_t sz = end - start;
    uint8_t* next = reinterpret_cast<uint8_t*>(memchr(start, '\r', sz));
    if (!next)
      break;

    if (next[1] == '\n') {
      next[0] = '\0';
      return next;
    }
    start = next + 1;
  }
  return nullptr;
}

}  // namespace

RespParser::RespParser() : write_start_(buf_.begin()) {
  buf_[kBufSz] = '\r';
  buf_[kBufSz + 1] = '\n';
  next_read_ = write_start_;
  next_parse_ = write_start_;
}

auto RespParser::ParseNext(absl::string_view* line) -> ParseStatus {
  if (!next_read_) {
    next_read_ = buf_.data();
    next_parse_ = next_read_;
  }

  if (write_start_ - next_parse_ < 2)
    return ParseStatus::MORE_DATA;

  uint8_t* tmp = ParseLine(next_parse_, write_start_);
  if (tmp) {
    *line = absl::string_view(reinterpret_cast<char*>(next_read_), tmp - next_read_);
    next_read_ = tmp + 2;
    next_parse_ = next_read_;
    // We can not realign here because we will ruin the *line.

    return LINE_FINISHED;
  }

  next_parse_ = write_start_ - 1;
  return MORE_DATA;
}

void RespParser::Realign() {
  ssize_t left_sz = write_start_ - next_read_;
  CHECK_GE(left_sz, 0);

  if (left_sz == 0) {
    write_start_ = buf_.data();
    next_read_ = nullptr;
  } else if (left_sz < 64) {
    memmove(buf_.data(), next_read_, left_sz);
    next_read_ = buf_.data();
    write_start_ = buf_.data() + left_sz;
    next_parse_ = next_read_;
  }
}

}  // namespace redis
