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

// Header is checksum (4 bytes), record length (Fixed32), type (1 byte) and
// optional "record specific header".
// kBlockHeaderSize summarizes lengths of checksum, the length and the type
// The type is an enum RecordType masked with kXXMask values (currently just kCompressedMask).
constexpr uint32 kBlockHeaderSize = 4 + 4 + 1;

struct Header {
  uint16_t multiplier = 1;
  uint16_t meta_count = 0;
};

util::Status ParseHeader(file::ReadonlyFile* f, Header* header);

enum RecordType : uint8_t {
  kZeroType = 0,  // Not used.

  kFullType = 1,

  // fragments
  kFirstType = 2,
  kMiddleType = 3,
  kLastType = 4,
  kArrayType = 5,
};
constexpr uint8 kMaxRecordVal = kArrayType;

constexpr uint8 kCompressedMask = 0x10;

}  // namespace lst2
}  // namespace file
