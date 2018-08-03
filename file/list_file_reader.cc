// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "file/list_file.h"

#include <cstdio>

#include "base/flags.h"
#include "base/varint.h"

#include "base/fixed.h"
#include "base/crc32c.h"
#include "file/compressors.h"

#include "absl/strings/match.h"

namespace file {

using util::Status;
using util::StatusCode;
using file::ReadonlyFile;
using std::string;
using strings::u8ptr;
using strings::AsString;
using strings::FromBuf;
using namespace ::util;
using namespace list_file;

ListReader::ListReader(file::ReadonlyFile* file, Ownership ownership, bool checksum,
                       CorruptionReporter reporter)
  : file_(file), ownership_(ownership), reporter_(reporter),
    checksum_(checksum) {
}

ListReader::ListReader(StringPiece filename, bool checksum, CorruptionReporter reporter)
    : ownership_(TAKE_OWNERSHIP), reporter_(reporter), checksum_(checksum) {
  auto res = ReadonlyFile::Open(filename);
  CHECK(res.ok()) << res.status << ", file name: " << filename;
  file_ = res.obj;
  CHECK(file_) << filename;
}

ListReader::~ListReader() {
  if (ownership_ == TAKE_OWNERSHIP) {
    auto st = file_->Close();
    if (!st.ok()) {
      LOG(WARNING) << "Error closing file, status " << st;
    }
    delete file_;
  }
}

bool ListReader::GetMetaData(std::map<std::string, std::string>* meta) {
  if (!ReadHeader()) return false;
  *meta = meta_;
  return true;
}

bool ListReader::ReadRecord(StringPiece* record, std::string* scratch) {
  if (!ReadHeader()) return false;

  scratch->clear();
  *record = StringPiece();
  bool in_fragmented_record = false;
  StringPiece fragment;

  while (true) {
    if (array_records_ > 0) {
      uint32 item_size = 0;
      const uint8* aend = reinterpret_cast<const uint8*>(array_store_.end());
      const uint8* item_ptr = Varint::Parse32WithLimit(u8ptr(array_store_), aend,
                                                       &item_size);
      const uint8* next_rec_ptr = item_ptr + item_size;
      if (item_ptr == nullptr || next_rec_ptr > aend) {
        ReportCorruption(array_store_.size(), "invalid array record");
        array_records_ = 0;
      } else {
        read_header_bytes_ += item_ptr - u8ptr(array_store_);
        array_store_.remove_prefix(next_rec_ptr - u8ptr(array_store_));
        *record = StringPiece(strings::charptr(item_ptr), item_size);
        read_data_bytes_ += item_size;
        --array_records_;
        return true;
      }
    }
    const unsigned int record_type = ReadPhysicalRecord(&fragment);
    switch (record_type) {
      case kFullType:
        if (in_fragmented_record) {
          ReportCorruption(scratch->size(), "partial record without end(1)");
        } else {
          scratch->clear();
          *record = fragment;

          return true;
        }
      case kFirstType:
        if (in_fragmented_record) {
          ReportCorruption(scratch->size(), "partial record without end(2)");
        }
        *scratch = AsString(fragment);
        in_fragmented_record = true;

        break;

      case kMiddleType:
        if (!in_fragmented_record) {
          ReportCorruption(fragment.size(),
                           "missing start of fragmented record(1)");
        } else {
          scratch->append(fragment.data(), fragment.size());
        }
        break;

      case kLastType:
        if (!in_fragmented_record) {
          ReportCorruption(fragment.size(),
                           "missing start of fragmented record(2)");
        } else {
          scratch->append(fragment.data(), fragment.size());
          *record = StringPiece(*scratch);
          read_data_bytes_ += record->size();
          // last_record_offset_ = prospective_record_offset;
          return true;
        }
        break;
      case kArrayType: {
        if (in_fragmented_record) {
          ReportCorruption(scratch->size(), "partial record without end(4)");
        }
        uint32 array_records = 0;
        const uint8* array_ptr = Varint::Parse32WithLimit(u8ptr(fragment),
              u8ptr(fragment) + fragment.size(), &array_records);
        if (array_ptr == nullptr || array_records == 0) {
          ReportCorruption(fragment.size(), "invalid array record");
        } else {
          read_header_bytes_ += array_ptr - u8ptr(fragment);
          array_records_ = array_records;
          array_store_ = FromBuf(array_ptr, fragment.end() - strings::charptr(array_ptr));
          VLOG(2) << "Read array with count " << array_records;
        }
      }
      break;
      case kEof:
        if (in_fragmented_record) {
          ReportCorruption(scratch->size(), "partial record without end(3)");
          scratch->clear();
        }
        return false;
      case kBadRecord:
        if (in_fragmented_record) {
          ReportCorruption(scratch->size(), "error in middle of record");
          in_fragmented_record = false;
          scratch->clear();
        }
        break;
      default: {
        char buf[40];
        snprintf(buf, sizeof(buf), "unknown record type %u", record_type);
        ReportCorruption(
            (fragment.size() + (in_fragmented_record ? scratch->size() : 0)),
            buf);
        in_fragmented_record = false;
        scratch->clear();
      }
    }
  }
  return true;
}

static const uint8* DecodeString(const uint8* ptr, const uint8* end, string* dest) {
  if (ptr == nullptr) return nullptr;
  uint32 string_sz = 0;
  ptr = Varint::Parse32WithLimit(ptr, end, &string_sz);
  if (ptr == nullptr || ptr + string_sz > end)
    return nullptr;
  const char* str = reinterpret_cast<const char*>(ptr);
  dest->assign(str, str + string_sz);
  return ptr + string_sz;
}

Status list_file::HeaderParser::Parse(
  file::ReadonlyFile* file, std::map<std::string, std::string>* meta) {
  uint8 buf[kListFileHeaderSize];
  auto res = file->Read(0, strings::MutableByteRange(buf, kListFileHeaderSize));
  if (!res.ok())
    return res.status;

  offset_ = kListFileHeaderSize;
  StringPiece header = FromBuf(buf, kListFileHeaderSize);

  if (res.obj != kListFileHeaderSize ||
      !absl::StartsWith(header, StringPiece(kMagicString, kMagicStringSize)) ||
      buf[kMagicStringSize] == 0 || buf[kMagicStringSize] > 100 ) {
    return Status(StatusCode::IO_ERROR, "Invalid header");
  }

  unsigned block_factor = buf[kMagicStringSize];
  if (buf[kMagicStringSize + 1] == kMetaExtension) {
    uint8 meta_header[8];
    auto res = file->Read(offset_, strings::MutableByteRange(meta_header, sizeof meta_header));
    if (!res.ok())
      return res.status;

    offset_ += res.obj;

    uint32 length = coding::DecodeFixed32(meta_header + 4);
    uint32 crc = crc32c::Unmask(coding::DecodeFixed32(meta_header));

    std::unique_ptr<uint8[]> meta_buf(new uint8[length]);
    res = file->Read(offset_, strings::MutableByteRange(meta_buf.get(), length));
    if (!res.ok())
      return res.status;
    CHECK_EQ(res.obj, length);

    offset_ += length;
    uint32 actual_crc = crc32c::Value(meta_buf.get(), length);
    if (crc != actual_crc) {
      LOG(ERROR) << "Corrupted meta data " << actual_crc << " vs2 " << crc;
      return Status("Bad meta crc");
    }

    const uint8* end = meta_buf.get() + length;
    const uint8* ptr = Varint::Parse32WithLimit(meta_buf.get(), end, &length);
    for (uint32 i = 0; i < length; ++i) {
      string key, val;
      ptr = DecodeString(ptr, end, &key);
      ptr = DecodeString(ptr, end, &val);
      if (ptr == nullptr) {
        return Status("Bad meta crc");
      }
      meta->emplace(std::move(key), std::move(val));
    }
  }
  block_multiplier_ = block_factor;

  return Status::OK;
}

bool ListReader::ReadHeader() {
  if (block_size_ != 0) return true;
  if (eof_) return false;

  list_file::HeaderParser parser;
  Status status = parser.Parse(file_, &meta_);

  if (!status.ok()) {
    LOG(ERROR) << "Error reading header " << status;
    ReportDrop(file_->Size(), status);
    eof_ = true;
    return false;
  }

  file_offset_ = read_header_bytes_ = parser.offset();
  block_size_ = parser.block_multiplier() * kBlockSizeFactor;

  CHECK_GT(block_size_, 0);
  backing_store_.reset(new uint8[block_size_]);
  uncompress_buf_.reset(new uint8[block_size_]);

  return true;
}

void ListReader::ReportCorruption(size_t bytes, const string& reason) {
  ReportDrop(bytes, Status(StatusCode::IO_ERROR, reason));
}

void ListReader::ReportDrop(size_t bytes, const Status& reason) {
  LOG(ERROR) << "ReportDrop: " << bytes << " "
             << " block buffer_size " << block_buffer_.size() << ", reason: " << reason;
  if (reporter_ /*&& end_of_buffer_offset_ >= initial_offset_ + block_buffer_.size() + bytes*/) {
    reporter_(bytes, reason);
  }
}

using strings::charptr;

unsigned int ListReader::ReadPhysicalRecord(StringPiece* result) {
  while (true) {
    // Should be <= but due to bug in ListWriter we leave it as < until all the prod files are
    // replaced.
    if (block_buffer_.size() < kBlockHeaderSize) {
      if (!eof_) {
        size_t fsize = file_->Size();
        auto res = file_->Read(file_offset_,
                               strings::MutableByteRange(backing_store_.get(), block_size_));
        VLOG(2) << "read_size: " << res.obj << ", status: " << res.status;
        if (!res.ok()) {
          ReportDrop(res.obj, res.status);
          eof_ = true;
          return kEof;
        }
        block_buffer_.reset(backing_store_.get(), res.obj);
        file_offset_ += block_buffer_.size();
        if (file_offset_ >= fsize) {
          eof_ = true;
        }
        continue;
      } else if (block_buffer_.empty()) {
        // End of file
        return kEof;
      } else {
        size_t drop_size = block_buffer_.size();
        block_buffer_.clear();
        ReportCorruption(drop_size, "truncated record at end of file");
        return kEof;
      }
    }

    // Parse the header
    const uint8* header = block_buffer_.data();
    const uint8 type = header[8];
    uint32 length = coding::DecodeFixed32(header + 4);
    read_header_bytes_ += kBlockHeaderSize;

    if (length == 0 && type == kZeroType) {
      size_t bs = block_buffer_.size();
      block_buffer_.clear();
      // Handle the case of when mistakenly written last kBlockHeaderSize bytes as empty record.
      if (bs != kBlockHeaderSize) {
        LOG(ERROR) << "Bug reading list file " << bs;
        return kBadRecord;
      }
      continue;
    }

    if (length + kBlockHeaderSize > block_buffer_.size()) {
      VLOG(1) << "Invalid length " << length << " file offset " << file_offset_
              << " block size " << block_buffer_.size() << " type " << int(type);
      size_t drop_size = block_buffer_.size();
      block_buffer_.clear();
      ReportCorruption(drop_size, "bad record length or truncated record at eof.");
      return kBadRecord;
    }

    const uint8* data_ptr = header + kBlockHeaderSize;
    // Check crc
    if (checksum_) {
      uint32_t expected_crc = crc32c::Unmask(coding::DecodeFixed32(header));
      // compute crc of the record and the type.
      uint32_t actual_crc = crc32c::Value(data_ptr - 1, 1 + length);
      if (actual_crc != expected_crc) {
        // Drop the rest of the buffer since "length" itself may have
        // been corrupted and if we trust it, we could find some
        // fragment of a real log record that just happens to look
        // like a valid log record.
        size_t drop_size = block_buffer_.size();
        block_buffer_.clear();
        ReportCorruption(drop_size, "checksum mismatch");
        return kBadRecord;
      }
    }
    uint32 record_size = length + kBlockHeaderSize;
    block_buffer_.advance(record_size);

    if (type & kCompressedMask) {
      if (!Uncompress(data_ptr, &length)) {
        ReportCorruption(record_size, "Uncompress failed.");
        return kBadRecord;
      }
      data_ptr = uncompress_buf_.get();
    }

    *result = FromBuf(data_ptr, length);
    return type & 0xF;
  }
}

bool ListReader::Uncompress(const uint8* data_ptr, uint32* size) {
  uint8 method = *data_ptr++;
  VLOG(2) << "Uncompress " << int(method) << " with size " << *size;

  uint32 inp_sz = *size - 1;

  UncompressFunction uncompr_func = GetUncompress(list_file::CompressMethod(method));

  if (!uncompr_func) {
    LOG(ERROR) << "Could not find uncompress method " << int(method);
    return false;
  }
  size_t uncompress_size = block_size_;
  Status status = uncompr_func(data_ptr, inp_sz, uncompress_buf_.get(), &uncompress_size);
  if (!status.ok()) {
    VLOG(1) << "Uncompress error: " << status;
    return false;
  }

  *size = uncompress_size;
  return true;
}

}  // namespace file
