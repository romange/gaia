// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "strings/escaping.h"

namespace strings {

using namespace std;

template <typename T, size_t N> bool _in(const T& t, const T (&arr) [N]) {
  return std::find(arr, arr + N, t) != arr + N;
}

inline bool IsValidUrlChar(char ch) {
  static constexpr char kArr[] = "-_.!~*'()";
  return absl::ascii_isalnum(ch) || _in(ch, kArr);
}

static size_t InternalUrlEncode(absl::string_view src, char* dest) {
  static const char digits[] = "0123456789ABCDEF";

  char* start = dest;
  for (char ch_c : src) {
    unsigned char ch = static_cast<unsigned char>(ch_c);
    if (IsValidUrlChar(ch)) {
      *dest++ = ch_c;
    } else {
      *dest++ = '%';
      *dest++ = digits[(ch >> 4) & 0x0F];
      *dest++ = digits[ch & 0x0F];
    }
  }
  *dest = 0;

  return static_cast<size_t>(dest - start);
}

void AppendEncodedUrl(const absl::string_view src, string* dest) {
  size_t sz = dest->size();
  dest->resize(dest->size() + src.size() * 3 + 1);
  char* next = &dest->front() + sz;
  size_t written = InternalUrlEncode(src, next);
  dest->resize(sz + written);
}

}  // namespace strings
