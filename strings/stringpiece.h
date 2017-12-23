// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <absl/strings/string_view.h>
#include <folly/Range.h>

typedef absl::string_view StringPiece;

namespace strings {

inline const char* charptr(const unsigned char* ptr) {
  return reinterpret_cast<const char*>(ptr);
}

inline char* charptr(unsigned char* ptr) {
  return reinterpret_cast<char*>(ptr);
}

using folly::MutableByteRange;
using folly::ByteRange;

inline ByteRange ToByteRange(StringPiece s) { 
  return ByteRange(reinterpret_cast<const uint8_t*>(s.data()), s.size());
}

inline MutableByteRange AsMutableByteRange(std::string& s) {
  return MutableByteRange(reinterpret_cast<uint8_t*>(&s.front()), s.size());
}

}  // namespace strings

