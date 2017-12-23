// Copyright 2014, .com .  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "strings/unique_strings.h"

std::pair<StringPiece, bool> UniqueStrings::Insert(StringPiece source) {
  auto it = db_.find(source);
  if (it != db_.end())
    return std::make_pair(*it, false);
  if (source.empty()) {
    auto res = db_.insert(StringPiece());
    return std::make_pair(*res.first, res.second);
  }
  char* str = arena_.Allocate(source.size());
  memcpy(str, source.data(), source.size());
  StringPiece val(str, source.size());
  db_.insert(val);
  return std::make_pair(val, true);
}