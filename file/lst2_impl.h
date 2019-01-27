// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include "file/list_file.h"
#include "file/list_file_format2.h"

namespace file {
namespace lst2 {

class Lst2Impl : public ListWriter::WriterImpl {
 public:
  Lst2Impl(util::Sink* sink, const ListWriter::Options& opts);
  ~Lst2Impl();

  util::Status Init(const std::map<std::string, std::string>& meta) final;
  util::Status AddRecord(StringPiece slice) final;
  util::Status Flush() final;

 private:
  util::Status EmitPhysicalRecord(list_file::RecordType type, const uint8* ptr, size_t length);

  void AddRecordToArray(StringPiece size_enc, StringPiece record);
  util::Status FlushArray();

  size_t block_size() const { return kBlockSizeFactor * options_.block_size_multiplier; }

  
  std::unique_ptr<uint8[]> array_store_;
  std::unique_ptr<uint8[]> compress_buf_;

  uint8 *array_next_ = nullptr, *array_end_ = nullptr;  // wraps array_store_
  bool init_called_ = false;

  uint32 array_records_ = 0;
  size_t block_offset_ = 0;  // Current offset in block
};

}  // namespace lst2
}  // namespace file
