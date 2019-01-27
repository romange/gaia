// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
// Based on leveldb journal implementation.
//
#include "file/lst2_impl.h"

#include "file/list_file_format2.h"

#include <crc32c/crc32c.h>

#include "base/fixed.h"
#include "file/filesource.h"

#include "base/coder.h"
#include "base/varint.h"
#include <lz4.h>

using strings::u8ptr;

using namespace ::util;
using std::string;

namespace file {
namespace lst2 {

constexpr char kMagicString[] = "LST2";
static_assert(kMagicStringSize == sizeof(kMagicString), "");

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

Lst2Impl::Lst2Impl(util::Sink* sink, const ListWriter::Options& opts)
    : WriterImpl(sink, opts) {
  CHECK_GT(options_.block_size_multiplier, 0);
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

/*
   Type_Flags(1) + (ArrayCount(2)) + RecordSize/CompressedSize(4) + (CRC*) 4 bytes + Payload.
   For compressed payload there is no need fror CRC check.
   Payload: (uint16_t item_sz[ArrayCount]) + ItemDataSize. // item_sz if item type is array.
            Another option: To store varint_payload_sz + varint_sz_arr + ItemDataSize
*/

}  // namespace lst2
}  // namespace file
