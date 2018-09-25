// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include "base/pod_array.h"

namespace util {
namespace rpc {

typedef base::PODArray<uint8_t> BufferType;

class Envelope {
 public:
  BufferType header, letter;

  void Clear() {
    header.clear();
    letter.clear();
  }

  void Resize(size_t hsz, size_t lsz) {
    header.resize(hsz);
    letter.resize(lsz);
  }
};

}  // namespace rpc
}  // namespace util

