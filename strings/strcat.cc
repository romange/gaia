// Copyright 2008 and onwards Google Inc.  All rights reserved.

#include "strings/strcat.h"

#include "base/logging.h"

char* StrAppend(char* dest, unsigned n, std::initializer_list<absl::string_view> list) {
  for (const auto& val : list) {
    if (val.size() >= n)
      break;
    memcpy(dest, val.data(), val.size());
    dest += val.size();
    n -= val.size();
  }
  *dest = '\0';
  return dest;
}
