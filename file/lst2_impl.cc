// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
// Based on leveldb journal implementation.
//
#include "file/lst2_impl.h"

#include "file/list_file_format2.h"

#include <crc32c/crc32c.h>
#include <lz4.h>

#include "base/endian.h"
#include "base/varint.h"

using strings::u8ptr;

using namespace ::util;
using std::string;

namespace file {
namespace lst2 {

constexpr char kMagicString[] = "LST2";
static_assert(kMagicStringSize == sizeof(kMagicString), "");

/* Block - divides files into equal parts of size BlockSize. Each Block size can be
   in range :64k-16MB.
   Each block consists of one or more records.
*/
uint8_t* RecordHeader::Write(uint8_t* dest) const {
  *dest++ = flags;
  if (compress_method != list_file::kCompressionNone) {
    *dest++ = compress_method;
  }
  if (arr_count) {
    LittleEndian::Store16(dest, arr_count);
    dest += sizeof(uint16_t);
  }
  LittleEndian::Store32(dest, size);
  dest += (2 + (size > 0xFFFF));
  LittleEndian::Store32(dest, crc);
  dest += sizeof(uint32_t);

  return dest;
}

Lst2Impl::Lst2Impl(util::Sink* sink, const ListWriter::Options& opts) : WriterImpl(sink, opts) {
  CHECK_GT(options_.block_size_multiplier, 0);
  block_size_ = kBlockSizeFactor * options_.block_size_multiplier;
  array_store_.reset(new uint8[block_size()]);

  if (opts.compress_method != list_file::kCompressionNone) {
    CHECK_EQ(opts.compress_method, list_file::kCompressionLZ4);
    LOG(FATAL) << "TBD";
    // compress_buf_.reset(new uint8[compress_buf_size_ + 1]);  // +1 for compression method byte.
  }

  if (opts.append) {
    LOG(FATAL) << "TBD";
  }
}

Lst2Impl::~Lst2Impl() {
  DCHECK_EQ(array_records_, 0) << "ListWriter::Flush() was not called!";
  CHECK(Flush().ok());
}

Status Lst2Impl::Init(const std::map<string, string>& meta) {
  if (!options_.append) {
    CHECK(!init_called_);

    if (meta.size() > 65000) {
      return Status("Meta map too large");
    }

    uint8_t buf[kListFileHeaderSize];
    memcpy(buf, kMagicString, sizeof(kMagicString));
    uint8_t* next = buf + sizeof(kMagicString);
    LittleEndian::Store16(next, options_.block_size_multiplier);
    next += sizeof(uint16_t);

    LittleEndian::Store16(next, meta.size());
    RETURN_IF_ERROR(dest_->Append(strings::ByteRange(buf, sizeof(buf))));

    block_offset_ = kListFileHeaderSize;

    for (const auto& k_v : meta) {
      RETURN_IF_ERROR(AddRecord(k_v.first));
      RETURN_IF_ERROR(AddRecord(k_v.second));
    }
    init_called_ = true;
  }
  return Status::OK;
}

/* Record Header:
   |Type_Flags(1) | CompressType(1)? | ArrayCount(2)? | RecordSize (2-3) |CRC(4)? |
   Followed by payload of length RecordSize.
   For compressed payload there is no need fror CRC check.
   Payload: (uint16_t item_sz[ArrayCount]) + ItemDataSize. // item_sz if item type is array.
            Another option: To store varint_payload_sz + varint_sz_arr + ItemDataSize
*/

Status Lst2Impl::AddRecord(StringPiece record) {
  CHECK(init_called_) << "ListWriter::Init was not called.";

  if (record.size() >= (1ULL << 30)) {
    return Status("Record too large");
  }

  if (array_records_ > 0) {
    if (record.size() + array_next_ < array_end_) {
      uint8_t* next = Varint::Encode32(array_next_, record.size());
      if (next + record.size() <= array_end_) {
        memcpy(next, record.data(), record.size());
        array_next_ = next + record.size();
        ++array_records_;
        if (array_records_ == kuint16max) {
          RETURN_IF_ERROR(FlushArray());
        }
        return Status::OK;
      }
    }
    RETURN_IF_ERROR(FlushArray());
  }
  DCHECK_EQ(0, array_records_);

  uint32_t block_left = block_size() - block_offset_;
  DCHECK_GT(block_left, RecordHeader::kSingleSmallSize);

  auto wrapped_size = RecordHeader::WrappedSize(record.size());

  if (wrapped_size + RecordHeader::kArrayMargin <= block_left) {
    array_next_ = array_store_.get() + RecordHeader::kMaxSize;
    array_end_ = array_store_.get() + block_left;
    array_next_ = Varint::Encode32(array_next_, record.size());
    memcpy(array_next_, record.data(), record.size());
    array_next_ += record.size();
    array_records_ = 1;
    return Status::OK;
  }

  if (wrapped_size <= block_left) {
    // We have space for one record in this block but not for the array.
    return EmitSingleRecord(kFullType, record);
  }
  return WriteFragmented(record);
}

util::Status Lst2Impl::WriteFragmented(StringPiece record) {
  RecordType t = kFirstType;
  RecordHeader rh;
  uint8_t buf[RecordHeader::kMaxSize];

  uint32_t block_left = block_size() - block_offset_;
  DCHECK_GT(block_left, RecordHeader::kSingleSmallSize);

  rh.size = RecordHeader::PayloadSize(block_left);
  rh.flags = t;

  DCHECK_EQ(list_file::kCompressionNone, options_.compress_method);

  while (true) {
    rh.crc = crc32c::Crc32c(record.data(), rh.size);
    uint8_t* next = rh.Write(buf);
    uint32_t hs = next - buf;
    strings::ByteRange br(strings::u8ptr(record.data()), rh.size);
    RETURN_IF_ERROR(EmitPhysicalRecord(strings::ByteRange(buf, hs), br));
    record.remove_prefix(rh.size);
    if (record.empty())
      break;

    DCHECK_EQ(0, block_offset_);
    rh.size = std::min<uint32_t>(RecordHeader::PayloadSize(block_size_), record.size());
    t = rh.size == record.size() ? kLastType : kMiddleType;
  }
  return Status::OK;
}

util::Status Lst2Impl::EmitPhysicalRecord(strings::ByteRange header, strings::ByteRange record) {
  DCHECK_LE(block_offset_ + header.size() + record.size(), block_size_);

  RETURN_IF_ERROR(dest_->Append(header));
  RETURN_IF_ERROR(dest_->Append(record));

  block_offset_ += (header.size() + record.size());

  if (block_offset_ + RecordHeader::kSingleSmallSize >= block_size_) {
    if (block_offset_ < block_size_) {
      uint8_t buf[RecordHeader::kSingleSmallSize] = {0};
      RETURN_IF_ERROR(dest_->Append(strings::ByteRange(buf, block_size_ - block_offset_)));
    }
    block_offset_ = 0;
  }
  return Status::OK;
}

}  // namespace lst2
}  // namespace file
