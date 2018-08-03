// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include "strings/range.h"
#include "absl/strings/string_view.h"

typedef absl::string_view StringPiece;

namespace strings {

inline const char* charptr(const unsigned char* ptr) {
  return reinterpret_cast<const char*>(ptr);
}

inline char* charptr(unsigned char* ptr) {
  return reinterpret_cast<char*>(ptr);
}

inline const uint8_t* u8ptr(const char* ptr) {
  return reinterpret_cast<const uint8_t*>(ptr);
}

inline const uint8_t* u8ptr(StringPiece s) {
  return reinterpret_cast<const uint8_t*>(s.data());
}


inline uint8_t* u8ptr(char* ptr) {
  return reinterpret_cast<uint8_t*>(ptr);
}

inline ByteRange ToByteRange(StringPiece s) {
  return ByteRange(reinterpret_cast<const uint8_t*>(s.data()), s.size());
}

inline MutableByteRange AsMutableByteRange(std::string& s) {
  return MutableByteRange(reinterpret_cast<uint8_t*>(&s.front()), s.size());
}

inline std::string AsString(StringPiece piece) { return std::string(piece.data(), piece.size()); }

inline StringPiece FromBuf(const uint8_t* ptr, size_t len) {
  return StringPiece(reinterpret_cast<const char*>(ptr), len);
}

}  // namespace strings

