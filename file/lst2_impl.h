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
  util::Status EmitPhysicalRecord(strings::ByteRange header, strings::ByteRange record);
  util::Status EmitSingleRecord(RecordType type, StringPiece record);
  util::Status WriteFragmented(StringPiece record);

  void AddRecordToArray(StringPiece size_enc, StringPiece record);
  util::Status FlushArray();

  uint32_t block_size() const { return block_size_; }

  std::unique_ptr<uint8[]> array_store_;
  std::unique_ptr<uint8[]> compress_buf_;

  uint8 *array_next_ = nullptr, *array_end_ = nullptr;  // wraps array_store_
  bool init_called_ = false;

  uint32_t block_offset_ = 0;  // Current offset in block
  uint32_t array_records_ = 0;
  uint32_t block_size_ = 0;
};


class ReaderImpl : public ListReader::FormatImpl {
 public:
  using FormatImpl::FormatImpl;

  bool ReadHeader(std::map<std::string, std::string>* dest) final;

  bool ReadRecord(StringPiece* record, std::string* scratch) final;

 private:
};

}  // namespace lst2
}  // namespace file
