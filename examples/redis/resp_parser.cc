// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "examples/redis/resp_parser.h"
#include "base/logging.h"

namespace redis {

namespace {

uint8_t* ParseLine(uint8_t* buf, const uint8_t* end, absl::string_view* res) {
  uint8_t* start = buf;

  while (start < end) {
    size_t sz = end - start;
    uint8_t* next = reinterpret_cast<uint8_t*>(memchr(start, '\r', sz));
    if (!next)
      break;

    if (next[1] == '\n') {
      next[0] = '\0';
      *res = absl::string_view{reinterpret_cast<char*>(buf), size_t(next - buf)};
      return next + 2;
    }
    start = next + 1;
  }
  return nullptr;
}

}  // namespace

auto RespParser::WriteCommit(size_t write_sz, absl::string_view* line) -> ParseStatus {
  next_line_ = write_start_;
  write_start_ += write_sz;

  next_line_ = ParseLine(next_line_, write_start_, line);
  return next_line_ ? LINE_FINISHED : MORE_DATA;
}

auto RespParser::ParseNext(absl::string_view* line) -> ParseStatus {
  if (!next_line_)
    return MORE_DATA;

  next_line_ = ParseLine(next_line_, write_start_, line);
  if (next_line_)
    return LINE_FINISHED;

  Realign();
  return MORE_DATA;
}

void RespParser::Realign() {
  ssize_t left_sz = write_start_ - next_line_;
  CHECK_GE(left_sz, 0);

  if (left_sz == 0) {
    write_start_ = buf_.data();
    next_line_ = nullptr;
  } else if (left_sz < 64) {
    memmove(buf_.data(), next_line_, left_sz);
    next_line_ = buf_.data();
    write_start_ = buf_.data() + left_sz;
  }

}


}  // namespace redis
