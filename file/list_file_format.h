// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Log format information shared by reader and writer.
// See ../doc/log_format.txt for more detail.

#ifndef _LIST_FILE_FORMAT_H_
#define _LIST_FILE_FORMAT_H_

#include <map>
#include "base/integral_types.h"
#include "file/file.h"
#include "util/status.h"

namespace file {
namespace list_file {

enum RecordType {
  kZeroType = 0,  // Not used.

  kFullType = 1,

  // For fragments
  kFirstType = 2,
  kMiddleType = 3,
  kArrayType = 4,
  kLastType = 5
};
constexpr uint8 kMaxRecordType = kLastType;

constexpr uint8 kCompressedMask = 0x10;

// Please note that in case of compression, the record header is followed by a byte describing
// the compression method. Right now only methods below are supported.
enum CompressMethod : uint8_t {
  kCompressionZlib = 2,
  kCompressionLZ4 = 3
};

// The file header is:
//    magic string "LST1\0",
//    uint8 block_size_multiplier;
//    uint8 extentsion_type;
constexpr uint8 kMagicStringSize = 5;
constexpr uint8 kListFileHeaderSize = kMagicStringSize + 2;
constexpr uint32 kBlockSizeFactor = 65536;
constexpr uint8 kNoExtension = 0;
constexpr uint8 kMetaExtension = 1;

// Header is checksum (4 bytes), record length (Fixed32), type (1 byte) and
// optional "record specific header".
// kBlockHeaderSize summarizes lengths of checksum, the length and the type
// The type is an enum RecordType masked with kXXMask values (currently just kCompressedMask).
constexpr uint32 kBlockHeaderSize = 4 + 4 + 1;

// Record of arraytype header is just varint32 that contains number of array records.
// Array header is followed by (varint32, data blob) pairs.
// Number of array records is limited to (1 << 28) - 1 = 268435455 so the number of items is at
// most 4 bytes.
constexpr uint32 kArrayRecordMaxHeaderSize = 5 /*Varint::kMax32*/ + kBlockHeaderSize;

extern const char kMagicString[];

class HeaderParser {
  unsigned offset_ = 0;
  unsigned block_multiplier_ = 0;
public:
  util::Status Parse(file::ReadonlyFile* f, std::map<std::string, std::string>* meta);

  unsigned offset() const { return offset_; }
  unsigned block_multiplier() const { return block_multiplier_; }
};

}  // namespace list_file
}  // namespace file

#endif  // _LIST_FILE_FORMAT_H_
