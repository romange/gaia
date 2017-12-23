// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <absl/strings/string_view.h>

typedef absl::string_view StringPiece;

namespace strings {

inline const char* charptr(const unsigned char* ptr) {
  return reinterpret_cast<const char*>(ptr);
}

inline char* charptr(unsigned char* ptr) {
  return reinterpret_cast<char*>(ptr);
}

}  // namespace strings

