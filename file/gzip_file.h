// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include "file/file.h"

extern "C" struct gzFile_s;

namespace file {

// Creates a gzipped file for writing.
class GzipFile : public WriteFile {
  gzFile_s* gz_file_ = nullptr;
  uint8 level_;
  GzipFile(StringPiece file_name, unsigned level);
public:
  static GzipFile* Create(StringPiece file_name, unsigned level = 2) {
    return new GzipFile(file_name, level);
  }

  bool Open() override;
  bool Close() override;

  util::Status Write(const uint8* buffer, uint64 length) override;
};

}  // namespace file
