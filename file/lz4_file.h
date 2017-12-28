// Copyright 2015, .com .  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include "file/file.h"

struct LZ4F_cctx_s;

namespace file {

// Creates a lz4-compressed file for writing.
class LZ4File : public WriteFile {
  LZ4F_cctx_s* context_ = nullptr;
  uint8* buf_ = nullptr;
  size_t buf_size_ = 0;

  uint8 level_ = 0;
  int fd_ = 0;

  LZ4File(StringPiece file_name, unsigned level);
  ~LZ4File();
public:
  static LZ4File* Create(StringPiece file_name, unsigned level = 2) {
    return new LZ4File(file_name, level);
  }

  bool Open() override;
  bool Close() override;

  base::Status Write(const uint8* buffer, uint64 length) override;
};


}  // namespace file
