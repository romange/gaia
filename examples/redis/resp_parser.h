// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include <array>

#include "absl/strings/string_view.h"
#include "absl/types/span.h"

namespace redis {

class RespParser {
  static constexpr uint16_t kBufSz = 256;

 public:
  using Buffer = absl::Span<uint8_t>;
  enum ParseStatus { LINE_FINISHED = 1, MORE_DATA = 2 };

  RespParser() : write_start_(buf_.begin()) {
    buf_[kBufSz] = '\r';
    buf_[kBufSz + 1] = '\n';
    next_line_ = write_start_;
  }

  //! 0-copy interface
  Buffer GetDestBuf() const {
    return Buffer(write_start_, buf_.data() + kBufSz - write_start_);
  }

  ParseStatus WriteCommit(size_t write_sz, absl::string_view* line);

  //! Consumes previously returned line.
  //! Returns true if another line was found in the write buffer and points line to it.
  //! Returns false if no EOL is present .
  ParseStatus ParseNext(absl::string_view* line);

  Buffer ReadBuf() const {
    return next_line_ ? Buffer(next_line_, write_start_ - next_line_) : Buffer{};
  }

  void Reset() {
    write_start_ = buf_.data();
    next_line_ = nullptr;
  }

  void Consume(size_t sz) {
    next_line_ += sz;
    Realign();
  }

 private:
  uint8_t* WriteEnd() {
    return buf_.data() + kBufSz;
  }
  void Realign();

  std::array<uint8_t, kBufSz + 4> buf_;
  uint8_t *write_start_, *next_line_ = nullptr;
};

}  // namespace redis
