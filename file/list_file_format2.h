// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <cstdint>
#include "file/file.h"

namespace file {
namespace lst2 {

// The file header is:
//    magic string "LST2\0",
//    uint16 block_size_multiplier;
//    uint16 header_string_count;
constexpr uint8_t kMagicStringSize = 5;
constexpr uint8_t kListFileHeaderSize = kMagicStringSize + sizeof(uint16_t) * 2;
constexpr uint32_t kBlockSizeFactor = 65536;

extern const char kMagicString[];

enum RecordType : uint8_t {
  kZeroType = 0,  // Not used.

  kFullType = 1,

  // fragments
  kFirstType = 2,
  kMiddleType = 3,
  kLastType = 4,
  kArrayType = 5,

  kMaxRecordVal = kArrayType,

  kRecordSize3BytesFlag = 0x08,
  kCompressedFlag = 0x10,
};

static_assert(RecordType::kMaxRecordVal < 8, "Must be 3 bits");

/*
   Record Header:
   |Type_Flags(1) | CompressType(1)? | ArrayCount(2)? | RecordSize (2-3) | CRC(4)? |
*/
class RecordHeader {
 public:
  uint8_t flags = 0;
  uint32_t crc = 0;
  uint32_t arr_count = 0;
  uint32_t size = 0;
  uint8_t compress_method = list_file::kCompressionNone;

  constexpr static uint32_t kMaxSize = 1 + 2 + 3 + 4;      // Non-compressed large array record.
  constexpr static uint32_t kSingleSmallSize = 1 + 2 + 4;  // Single small record.
  constexpr static uint32_t kArrayMargin = 12;

  uint8_t* Write(uint8_t* dest) const;

  // src must point to at serialized record header with enough buffer memory to hold kMaxSize.
  // We guarantee this by reserving more memory than needed thus protecting against corruptions.
  // Returns number of bytes parsed.
  size_t Parse(const uint8_t* src);

  // Size including the header for the single record.
  static uint32_t WrappedSize(uint32_t ps) { return kSingleSmallSize + (ps > kuint16max) + ps; }

  // Inverse of WrappedSize. Size for a payload given the space for the wrapped size.
  static uint32_t PayloadSize(uint32_t space) {
    space -= kSingleSmallSize;

    return space > 0xFFFF ? space - 1 : space;
  }
};

}  // namespace lst2
}  // namespace file
