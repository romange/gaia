// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <memory>
#include "util/sinksource.h"

namespace util {

class ZStdSink : public Sink {
 public:
  // Takes ownership over upstream.
  ZStdSink(Sink* upstream);
  ~ZStdSink();

  Status Init(int level);
  Status Append(const strings::ByteRange& slice) override;
  Status Flush() override;
  static size_t CompressBound(size_t src_size);

 private:
  size_t buf_sz_;
  std::unique_ptr<uint8_t[]> buf_;
  std::unique_ptr<Sink> upstream_;
  void* zstd_handle_;
};

class ZStdSource : public Source {
 public:
  explicit ZStdSource(Source* upstream);
  ~ZStdSource();
  static bool HasValidHeader(Source* upstream);

 private:
  StatusObject<size_t> ReadInternal(const strings::MutableByteRange& range) override;

  std::unique_ptr<Source> sub_stream_;
  void* zstd_handle_;
  std::unique_ptr<uint8_t[]> buf_;
  strings::MutableByteRange buf_range_;
};


}  // namespace util
