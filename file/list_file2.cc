// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "file/list_file_format2.h"

#include <crc32c/crc32c.h>

#include "base/fixed.h"
#include "file/filesource.h"
#include "file/list_file.h"

#include "base/coder.h"
#include "base/varint.h"

using strings::u8ptr;

using namespace ::util;
using std::string;

namespace file {
namespace lst2 {

constexpr char kMagicString[] = "LST2";

class BlockHeader {
  uint8 buf_[kBlockHeaderSize];

 public:
  BlockHeader(RecordType type) { buf_[8] = type; }

  void EnableCompression() { buf_[8] |= kCompressedMask; }

  void SetCrcAndLength(const uint8* ptr, size_t length);

  Status Write(util::Sink* sink) const {
    return sink->Append(strings::ByteRange(buf_, kBlockHeaderSize));
  }
};

void BlockHeader::SetCrcAndLength(const uint8* ptr, size_t length) {
  coding::EncodeFixed32(length, buf_ + 4);

  // Compute the crc of the record type and the payload.
  uint32_t crc = crc32c::Crc32c(buf_ + 8, 1);
  crc = crc32c::Extend(crc, ptr, length);
  coding::EncodeFixed32(crc, buf_);
}

class Lst2Impl : public ListWriter::WriterImpl {
 public:
  Lst2Impl(util::Sink* sink, const ListWriter::Options& opts);
  ~Lst2Impl();

  Status Init(const std::map<string, string>& meta) final;
  Status AddRecord(StringPiece slice) final;
  Status Flush() final;

 private:
  util::Status EmitPhysicalRecord(list_file::RecordType type, const uint8* ptr, size_t length);

  uint32 block_leftover() const { return block_leftover_; }

  void AddRecordToArray(StringPiece size_enc, StringPiece record);
  util::Status FlushArray();

  std::unique_ptr<util::Sink> dest_;
  std::unique_ptr<uint8[]> array_store_;
  std::unique_ptr<uint8[]> compress_buf_;

  uint8 *array_next_ = nullptr, *array_end_ = nullptr;  // wraps array_store_
  bool init_called_ = false;

  uint32 array_records_ = 0;
  uint32 block_offset_ = 0;  // Current offset in block

  uint32 block_size_ = 0;
  uint32 block_leftover_ = 0;
  size_t compress_buf_size_ = 0;
};

Lst2Impl::Lst2Impl(util::Sink* sink, const ListWriter::Options& opts)
    : WriterImpl(opts), dest_(sink) {
  block_size_ = kBlockSizeFactor * opts.block_size_multiplier;
  array_store_.reset(new uint8[block_size_]);
  block_leftover_ = block_size_;

  if (opts.compress_method != list_file::kCompressionNone) {
    CHECK_EQ(opts.compress_method, list_file::kCompressionLZ4);
    LOG(FATAL) << "TBD";
    compress_buf_.reset(new uint8[compress_buf_size_ + 1]);  // +1 for compression method byte.
  }

  if (opts.append) {
    block_leftover_ = block_size_ - (opts.internal_append_offset % block_size_);
  }
}

Lst2Impl::~Lst2Impl() {
  DCHECK_EQ(array_records_, 0) << "ListWriter::Flush() was not called!";
  CHECK(Flush().ok());
}

}  // namespace lst2
}  // namespace file
