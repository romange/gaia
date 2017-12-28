// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "file/list_file.h"

#include <zlib.h>

#include "file/filesource.h"
#include "file/file_util.h"
#include "base/fixed.h"

#include "base/varint.h"
#include "base/crc32c.h"
#include "base/coder.h"

using strings::ByteRange;
using util::Status;
using util::StatusCode;

namespace file {

/*cmprss::Method ComprMethod(uint8 m) {
  switch (m) {
    case list_file::kCompressionZlib:
      return cmprss::ZLIB_METHOD;
    case list_file::kCompressionLZ4:
      return cmprss::LZ4_METHOD;    
  }
  LOG(ERROR) << "Unknown compression " << m;
  return cmprss::UNKNOWN_METHOD;
}
*/

namespace list_file {


const char kMagicString[] = "LST1";

class BlockHeader {
  uint8 buf_[kBlockHeaderSize];
public:
  BlockHeader(RecordType type) {
    buf_[8] = type;
  }

  void EnableCompression() { buf_[8] |= kCompressedMask; }

  void SetCrcAndLength(const uint8* ptr, size_t length);

  Status Write(util::Sink* sink) const {
    return sink->Append(strings::StringPiece(buf_, kBlockHeaderSize));
  }
};


void BlockHeader::SetCrcAndLength(const uint8* ptr, size_t length) {
  coding::EncodeFixed32(length, buf_ + 4);

  // Compute the crc of the record type and the payload.
  uint32 crc = crc32c::Value(buf_ + 8, 1);
  crc = crc32c::Extend(crc, ptr, length);
  crc = crc32c::Mask(crc);                 // Adjust for storage
  VLOG(2) << "EmitPhysicalRecord: type " << (buf_[8] & 0xF) <<  ", length: " << length
          << ", crc: " << crc << " compressed: " << (buf_[8] & kCompressedMask);

  coding::EncodeFixed32(crc, buf_);
}

}  // namespace list_file

using namespace ::util;
using std::string;

using namespace list_file;

namespace {

// We actually apply compression if the compressed size is less than
// 1 - 1/kCompressReduction of the original size.
constexpr unsigned kCompressReduction = 8;  // Currently we require 12.5% reduction.

class Varint32Encoder {
  uint8 buf_[Varint::kMax32];
  uint8 sz_ = 0;
public:
  Varint32Encoder(uint32 val = 0) {
    encode(val);
  }

  StringPiece slice() const { return StringPiece(buf_, sz_); }
  uint8 size() const { return sz_; }
  void encode(uint32 val) {
    sz_ = Varint::Encode32(buf_, val) - buf_;
  }
  const uint8* data() const { return buf_; }
};

class FileHeader {
  uint8 buf_[kListFileHeaderSize];
  const std::map<string, string>& meta_;

  util::GrowableEncoder encoder_;
public:
  FileHeader(uint8 multiplier, const std::map<string, string>& meta) : meta_(meta) {
    memcpy(buf_, kMagicString, kMagicStringSize);
    buf_[kMagicStringSize] = multiplier;
    buf_[kMagicStringSize + 1] = meta.empty() ? kNoExtension : kMetaExtension;
  }

  Status Write(util::Sink* sink) {
    StringPiece pc(buf_, sizeof(buf_));

    RETURN_IF_ERROR(sink->Append(pc));
    if (!meta_.empty()) {
      // Meta format: crc32, fixed32 - meta block size, varint32 map size,
      // (varint string size, string data)+
      // We do not bother with memory optimizations since the meta data should relatively small.
      // We do not bother with memory optimizations since the meta data should relatively small.
      encoder_.put_varint32(meta_.size());
      for (const auto& k_v : meta_) {
        encoder_.put_varint32(k_v.first.size());
        encoder_.put_string(k_v.first);
        encoder_.put_varint32(k_v.second.size());
        encoder_.put_string(k_v.second);
      }

      uint8 meta_header[8];
      coding::EncodeFixed32(encoder_.size(), meta_header + 4);
      uint32 crc = crc32c::Mask(crc32c::Value(encoder_.byteptr(), encoder_.size()));
      coding::EncodeFixed32(crc, meta_header);

      RETURN_IF_ERROR(sink->Append(ByteRange(meta_header, sizeof meta_header)));
      RETURN_IF_ERROR(sink->Append(ByteRange{encoder_.byteptr(), encoder_.size()}));
    }
    return Status::OK;
  }
};

}  // namespace

ListWriter::ListWriter(StringPiece filename, const Options& options)
    : options_(options) {
  size_t header_offset = 0;
  size_t file_offset = 0;

  if (options_.append) {
    auto status_obj = ReadonlyFile::Open(filename);
    if (status_obj.ok()) {
      HeaderParser parser;
      std::map<string, string> meta;
      if (parser.Parse(status_obj.obj, &meta).ok()) {
        options_.block_size_multiplier = parser.block_multiplier();
        header_offset = parser.offset();
        file_offset = status_obj.obj->Size();
      }
      status_obj.obj->Close();
      delete status_obj.obj;
    }
  }

  options_.append = header_offset > 0;
  file::OpenOptions open_options;
  open_options.append = options_.append;
  WriteFile* file = file::Open(filename, open_options);
  dest_.reset(new Sink(file, TAKE_OWNERSHIP));

  Construct();
  if (options_.append) {
    CHECK_GE(file_offset, header_offset);
    block_leftover_ = block_size_ - (file_offset - header_offset) % block_size_;
  }
}

ListWriter::ListWriter(util::Sink* dest, const Options& options)
   : dest_(dest), options_(options) {
  Construct();
}

void ListWriter::Construct() {
  block_size_ = kBlockSizeFactor * options_.block_size_multiplier;
  array_store_.reset(new uint8[block_size_]);
  block_leftover_ = block_size_;

  if (options_.use_compression) {
    CompressBoundFunction bound_f = GetCompressBound(options_.compress_method);
    compress_func_ = GetCompress(options_.compress_method);
    CHECK(bound_f && compress_func_);
    compress_buf_size_ = bound_f(block_size_);
    
    compress_buf_.reset(new uint8[compress_buf_size_ + 1]); // +1 for compression method byte.
  }
}

ListWriter::~ListWriter() {
  DCHECK_EQ(array_records_, 0) << "ListWriter::Flush() was not called!";
  CHECK(Flush().ok());
}

// Adds user provided meta information about the file. Must be called before Init.
void ListWriter::AddMeta(StringPiece key, StringPiece value) {
  CHECK(!init_called_);
  meta_[key.as_string()] = value.as_string();
}

Status ListWriter::Init() {
  if (!options_.append) {
    CHECK_GT(options_.block_size_multiplier, 0);
    CHECK(!init_called_);
    FileHeader header(options_.block_size_multiplier, meta_);

    RETURN_IF_ERROR(header.Write(dest_.get()));
    init_called_ = true;
  }
  return Status::OK;
}

inline void ListWriter::AddRecordToArray(StringPiece size_enc, StringPiece record) {
  memcpy(array_next_, size_enc.data(), size_enc.size());
  memcpy(array_next_ + size_enc.size(), record.data(), record.size());
  array_next_ += size_enc.size() + record.size();
  ++array_records_;
}

inline Status ListWriter::FlushArray() {
  if (array_records_ == 0) return Status::OK;

  Varint32Encoder enc(array_records_);

  // We prepend array_records_ integer right before the data, for that we skip
  //  kArrayRecordMaxHeaderSize - enc.size() bytes.
  uint8* start = array_store_.get() + kArrayRecordMaxHeaderSize - enc.size();
  memcpy(start, enc.data(), enc.size());

  // Flush the array.
  Status st = EmitPhysicalRecord(kArrayType, start, array_next_ - start);
  array_records_ = 0;
  return st;
}

Status ListWriter::AddRecord(StringPiece record) {
  CHECK_GT(block_size_, 0) << "ListWriter::Init was not called.";

  Varint32Encoder record_size_encoded(record.size());
  const uint32 record_size_total = record_size_encoded.size() + record.size();
  // Try to accomodate either in the array or a single block.  Multiple iterations might be
  // needed since we might fragment the record.
  bool fragmenting = false;
  ++records_added_;
  while (true) {
    if (array_records_ > 0) {
      if (array_next_ + record_size_total <= array_end_) {
        AddRecordToArray(record_size_encoded.slice(), record);
        return Status::OK;
      }
      RETURN_IF_ERROR(FlushArray());
      // Also we must either split the record or transfer to the next block.
    }
    if (block_leftover() <= kBlockHeaderSize) {
      // Block trailing bytes. Just fill them with zeroes.
      uint8 kBlockFilling[kBlockHeaderSize] = {0};
      RETURN_IF_ERROR(dest_->Append(StringPiece(kBlockFilling, block_leftover())));
      block_offset_ = 0;
      block_leftover_ = block_size_;
    }

    if (fragmenting) {
      size_t fragment_length = record.size();
      RecordType type = kLastType;
      if (fragment_length > block_leftover() - kBlockHeaderSize) {
        fragment_length = block_leftover() - kBlockHeaderSize;
        type = kMiddleType;
      }
      RETURN_IF_ERROR(EmitPhysicalRecord(type, record.ubuf(), fragment_length));
      if (type == kLastType)
        return Status::OK;
      record.advance(fragment_length);
      continue;
    }
    if (record_size_total + kArrayRecordMaxHeaderSize < block_leftover()) {
      // Lets start the array accumulation.
      // We leave space at the beginning to prepend the header at the end.
      array_next_ = array_store_.get() + kArrayRecordMaxHeaderSize;
      array_end_ = array_store_.get() + block_leftover();
      AddRecordToArray(record_size_encoded.slice(), record);
      return Status::OK;
    }
    if (kBlockHeaderSize + record.size() <= block_leftover()) {
      // We have space for one record in this block but not for the array.
      return EmitPhysicalRecord(kFullType, record.ubuf(), record.size());
    }
    // We must fragment.
    fragmenting = true;
    const size_t fragment_length = block_leftover() - kBlockHeaderSize;
    RETURN_IF_ERROR(EmitPhysicalRecord(kFirstType, record.ubuf(), fragment_length));
    record.advance(fragment_length);
  };
  return Status(StatusCode::INTERNAL_ERROR, "Should not reach here");
}

Status ListWriter::Flush() {
  return FlushArray();
}

using strings::charptr;

Status ListWriter::EmitPhysicalRecord(RecordType type, const uint8* ptr, size_t length) {
  DCHECK_LE(kBlockHeaderSize + length, block_leftover());

  // Format the header
  BlockHeader block_header(type);

  if (options_.use_compression && length > 64) {   
    size_t compressed_length = compress_buf_size_;
    auto status = compress_func_(options_.compress_level, ptr, length,
                                 compress_buf_.get() + 1, &compressed_length);
    if (status.ok()) {
      VLOG(1) << "Compressed record with size " << length << " to ratio "
            << float(compressed_length) / length;
      if (compressed_length < length - length / kCompressReduction) {
        block_header.EnableCompression();

        compress_buf_[0] = options_.compress_method;
        ptr = compress_buf_.get();
        compression_savings_ += (length - compressed_length);
        length = compressed_length + 1;
      }      
    }
  }
  block_header.SetCrcAndLength(ptr, length);

  // Write the header and the payload
  RETURN_IF_ERROR(block_header.Write(dest_.get()));
  RETURN_IF_ERROR(dest_->Append(StringPiece(strings::ByteRange(ptr, length))));

  bytes_added_ += (kBlockHeaderSize + length);
  block_offset_ += (kBlockHeaderSize + length);
  block_leftover_ = block_size_ - block_offset_;
  return Status::OK;
}

}  // namespace file
