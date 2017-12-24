// Copyright 2016, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "strings/strpmr.h"
#include "base/logging.h"

namespace strings {

StringPiece DeepCopy(StringPiece src, pmr::memory_resource* mr) {
  DCHECK(mr);
  if (src.empty())
    return src;
  char* dest = (char*)mr->allocate(src.size());
  memcpy(dest, src.data(), src.size());
  return StringPiece(dest, src.size());
}

std::pair<StringPiece, bool> StringPieceSet::Insert(StringPiece source) {
  assert(!source.empty());

  auto it = db_.find(source);
  if (it != db_.end())
    return std::make_pair(*it, false);
  char* str = (char*)arena_.allocate(source.size());
  memcpy(str, source.data(), source.size());
  StringPiece val(str, source.size());
  db_.insert(val);
  return std::make_pair(val, true);
}

}  // namespace strings
