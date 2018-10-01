// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include "base/pod_array.h"
#include "util/asio/asio_utils.h"

namespace util {
namespace rpc {

typedef base::PODArray<uint8_t> BufferType;

class Envelope {
 public:
  BufferType header, letter;

  Envelope() = default;
  Envelope(Envelope&&) = default;

  Envelope(size_t hsz, size_t lsz) {
    Resize(hsz, lsz);
  }

  void Clear() {
    header.clear();
    letter.clear();
  }

  void Resize(size_t hsz, size_t lsz) {
    header.resize(hsz);
    letter.resize(lsz);
  }

  auto buf_seq() { return make_buffer_seq(header, letter); }

  void Swap(Envelope* other) {
    if (other != this) {
      other->header.swap(header);
      other->letter.swap(letter);
    }
  }
};

}  // namespace rpc
}  // namespace util

