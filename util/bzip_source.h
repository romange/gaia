// Copyright 2013, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#ifndef BZIP_SOURCE_H
#define BZIP_SOURCE_H

#include <memory>
#include "util/sinksource.h"

namespace util {

class BzipSource : public Source {
public:
  // Takes ownership over sub_source
  BzipSource(Source* sub_source);
  ~BzipSource() override;

  static bool IsBzipSource(Source* source);
private:
  StatusObject<size_t> ReadInternal(const strings::MutableByteRange& range) override;

  struct Rep;

  std::unique_ptr<Source> sub_stream_;
  std::unique_ptr<Rep> rep_;
};

}  // namespace util

#endif  // BZIP_SOURCE_H
