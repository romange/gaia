// Copyright 2013, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/sinksource.h"
#include "base/logging.h"
#include "base/port.h"

namespace util {

Sink::WritableBuffer Sink::GetAppendBuffer(
    size_t min_capacity,
    WritableBuffer scratch,
    size_t /*desired_capacity_hint*/) {
  CHECK_GE(scratch.size(), min_capacity);
  return scratch;
}

Status Sink::Flush() { return Status::OK; }


StatusObject<size_t> Source::Read(const strings::MutableByteRange& range) {
  CHECK(!range.empty());

  if (prepend_buf_.size() >= range.size()) {
    memcpy(range.begin(), prepend_buf_.begin(), range.size());
    auto src = prepend_buf_.begin() + range.size();
    size_t new_size = prepend_buf_.size() - range.size();
    memmove(prepend_buf_.begin(), src, new_size);
    prepend_buf_.resize(new_size);

    return range.size();
  }

  size_t read = 0;
  if (!prepend_buf_.empty()) {
    memcpy(range.begin(), prepend_buf_.begin(), prepend_buf_.size());

    read = prepend_buf_.size();
    prepend_buf_.clear();

    DCHECK_LT(read, range.size());
  }
  auto res = ReadInternal(range.subpiece(read));
  if (!res.ok())
    return res;
  return res.obj + read;
}


StatusObject<size_t> StringSource::ReadInternal(const strings::MutableByteRange& range) {
  size_t to_fill = std::min<size_t>({range.size(), block_size_, input_.size()});
  memcpy(range.begin(), input_.begin(), to_fill);

  input_.remove_prefix(to_fill);
  return to_fill;
}

}  // namespace util

