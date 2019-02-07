// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
// Based on LevelDB implementation.
#include "file/list_file_reader.h"

#include <cstdio>

#include "base/flags.h"
#include "base/varint.h"

#include "base/crc32c.h"
#include "base/fixed.h"
#include "file/compressors.h"
#include "file/lst2_impl.h"
#include "absl/strings/match.h"

namespace file {

using file::ReadonlyFile;
using std::string;
using strings::AsString;
using strings::FromBuf;
using strings::u8ptr;
using util::Status;
using util::StatusCode;
using namespace ::util;

namespace {

class Lst1Impl : public ListReader::FormatImpl {
 public:
  using FormatImpl::FormatImpl;

  bool ReadHeader(std::map<std::string, std::string>* dest) final;

  bool ReadRecord(StringPiece* record, std::string* scratch) final;

 private:
  // Return type, or one of the preceding special values
  unsigned int ReadPhysicalRecord(StringPiece* result);

  // 'size' is size of the compressed blob.
  // Returns true if succeeded. In that case uncompress_buf_ will contain the uncompressed data
  // and size will be updated to the uncompressed size.
  bool Uncompress(const uint8* data_ptr, uint32* size);

  StringPiece array_store_;

  // Extend record types with the following special values
  enum {
    kEof = list_file::kMaxRecordType + 1,
    // Returned whenever we find an invalid physical record.
    // Currently there are three situations in which this happens:
    // * The record has an invalid CRC (ReadPhysicalRecord reports a drop)
    // * The record is a 0-length record (No drop is reported)
    // * The record is below constructor's initial_offset (No drop is reported)
    kBadRecord = list_file::kMaxRecordType + 2
  };
};

bool Lst1Impl::ReadHeader(std::map<std::string, std::string>* dest) {
  list_file::HeaderParser parser;
  Status status = parser.Parse(wrapper_->file, dest);

  if (!status.ok()) {
    wrapper_->BadHeader(status);
    return false;
  }

  wrapper_->file_offset_ = wrapper_->read_header_bytes = parser.offset();
  wrapper_->block_size = parser.block_multiplier() * list_file::kBlockSizeFactor;

  CHECK_GT(wrapper_->block_size, 0);
  wrapper_->backing_store_.reset(new uint8[wrapper_->block_size]);
  wrapper_->uncompress_buf_.reset(new uint8[wrapper_->block_size]);
  if (wrapper_->file_offset_ >= wrapper_->file->Size()) {
    wrapper_->eof = true;
  }
  return true;
}

bool Lst1Impl::ReadRecord(StringPiece* record, std::string* scratch) {
  scratch->clear();
  *record = StringPiece();
  bool in_fragmented_record = false;
  StringPiece fragment;
  using namespace list_file;

  while (true) {
    if (wrapper_->array_records > 0) {
      uint32 item_size = 0;
      const uint8* aend = reinterpret_cast<const uint8*>(array_store_.end());
      const uint8* item_ptr = Varint::Parse32WithLimit(u8ptr(array_store_), aend, &item_size);
      DVLOG(2) << "Array record with size: " << item_size;

      const uint8* next_rec_ptr = item_ptr + item_size;
      if (item_ptr == nullptr || next_rec_ptr > aend) {
        wrapper_->ReportCorruption(array_store_.size(), "invalid array record");
        wrapper_->array_records = 0;
      } else {
        wrapper_->read_header_bytes += item_ptr - u8ptr(array_store_);
        array_store_.remove_prefix(next_rec_ptr - u8ptr(array_store_));
        *record = StringPiece(strings::charptr(item_ptr), item_size);
        wrapper_->read_data_bytes += item_size;
        --wrapper_->array_records;
        return true;
      }
    }
    const unsigned int record_type = ReadPhysicalRecord(&fragment);
    switch (record_type) {
      case kFullType:
        if (in_fragmented_record) {
          wrapper_->ReportCorruption(scratch->size(), "partial record without end(1)");
        } else {
          scratch->clear();
          *record = fragment;

          return true;
        }
        ABSL_FALLTHROUGH_INTENDED;
      case kFirstType:
        if (in_fragmented_record) {
          wrapper_->ReportCorruption(scratch->size(), "partial record without end(2)");
        }
        *scratch = AsString(fragment);
        in_fragmented_record = true;

        break;

      case kMiddleType:
        if (!in_fragmented_record) {
          wrapper_->ReportCorruption(fragment.size(), "missing start of fragmented record(1)");
        } else {
          scratch->append(fragment.data(), fragment.size());
        }
        break;

      case kLastType:
        if (!in_fragmented_record) {
          wrapper_->ReportCorruption(fragment.size(), "missing start of fragmented record(2)");
        } else {
          scratch->append(fragment.data(), fragment.size());
          *record = StringPiece(*scratch);
          wrapper_->read_data_bytes += record->size();
          // last_record_offset_ = prospective_record_offset;
          return true;
        }
        break;
      case kArrayType: {
        if (in_fragmented_record) {
          wrapper_->ReportCorruption(scratch->size(), "partial record without end(4)");
        }
        uint32 array_records = 0;
        const uint8* array_ptr = Varint::Parse32WithLimit(
            u8ptr(fragment), u8ptr(fragment) + fragment.size(), &array_records);
        if (array_ptr == nullptr || array_records == 0) {
          wrapper_->ReportCorruption(fragment.size(), "invalid array record");
        } else {
          wrapper_->read_header_bytes += array_ptr - u8ptr(fragment);
          wrapper_->array_records = array_records;
          array_store_ = FromBuf(array_ptr, fragment.end() - strings::charptr(array_ptr));
          VLOG(2) << "Read array with count " << array_records;
        }
      } break;
      case kEof:
        if (in_fragmented_record) {
          wrapper_->ReportCorruption(scratch->size(), "partial record without end(3)");
          scratch->clear();
        }
        return false;
      case kBadRecord:
        if (in_fragmented_record) {
          wrapper_->ReportCorruption(scratch->size(), "error in middle of record");
          in_fragmented_record = false;
          scratch->clear();
        }
        break;
      default: {
        char buf[40];
        snprintf(buf, sizeof(buf), "unknown record type %u", record_type);
        wrapper_->ReportCorruption((fragment.size() + (in_fragmented_record ? scratch->size() : 0)),
                                   buf);
        in_fragmented_record = false;
        scratch->clear();
      }
    }
  }
  return true;
}

unsigned int Lst1Impl::ReadPhysicalRecord(StringPiece* result) {
  using list_file::kBlockHeaderSize;
  while (true) {
    if (wrapper_->block_buffer_.size() <= kBlockHeaderSize) {
      if (!wrapper_->eof) {
        size_t fsize = wrapper_->file->Size();
        strings::MutableByteRange mbr(wrapper_->backing_store_.get(), wrapper_->block_size);
        auto res = wrapper_->file->Read(wrapper_->file_offset_, mbr);
        VLOG(2) << "read_size: " << res.obj << ", status: " << res.status;
        if (!res.ok()) {
          wrapper_->ReportDrop(res.obj, res.status);
          wrapper_->eof = true;
          return kEof;
        }
        wrapper_->block_buffer_.reset(wrapper_->backing_store_.get(), res.obj);
        wrapper_->file_offset_ += wrapper_->block_buffer_.size();
        if (wrapper_->file_offset_ >= fsize) {
          wrapper_->eof = true;
        }
        continue;
      } else if (wrapper_->block_buffer_.empty()) {
        // End of file
        return kEof;
      } else {
        size_t drop_size = wrapper_->block_buffer_.size();
        wrapper_->block_buffer_.clear();
        wrapper_->ReportCorruption(drop_size, "truncated record at end of file");
        return kEof;
      }
    }

    // Parse the header
    const uint8* header = wrapper_->block_buffer_.data();
    const uint8 type = header[8];
    uint32 length = coding::DecodeFixed32(header + 4);
    wrapper_->read_header_bytes += kBlockHeaderSize;

    if (length == 0 && type == list_file::kZeroType) {
      size_t bs = wrapper_->block_buffer_.size();
      wrapper_->block_buffer_.clear();
      // Handle the case of when mistakenly written last kBlockHeaderSize bytes as empty record.
      if (bs != kBlockHeaderSize) {
        LOG(ERROR) << "Bug reading list file " << bs;
        return kBadRecord;
      }
      continue;
    }

    if (length + kBlockHeaderSize > wrapper_->block_buffer_.size()) {
      VLOG(1) << "Invalid length " << length << " file offset " << wrapper_->file_offset_
              << " block size " << wrapper_->block_buffer_.size() << " type " << int(type);
      size_t drop_size = wrapper_->block_buffer_.size();
      wrapper_->block_buffer_.clear();
      wrapper_->ReportCorruption(drop_size, "bad record length or truncated record at eof.");
      return kBadRecord;
    }

    const uint8* data_ptr = header + kBlockHeaderSize;
    // Check crc
    if (wrapper_->checksum) {
      uint32_t expected_crc = crc32c::Unmask(coding::DecodeFixed32(header));
      // compute crc of the record and the type.
      uint32_t actual_crc = crc32c::Value(data_ptr - 1, 1 + length);
      if (actual_crc != expected_crc) {
        // Drop the rest of the buffer since "length" itself may have
        // been corrupted and if we trust it, we could find some
        // fragment of a real log record that just happens to look
        // like a valid log record.
        size_t drop_size = wrapper_->block_buffer_.size();
        wrapper_->block_buffer_.clear();
        wrapper_->ReportCorruption(drop_size, "checksum mismatch");
        return kBadRecord;
      }
    }
    uint32 record_size = length + kBlockHeaderSize;
    wrapper_->block_buffer_.advance(record_size);

    if (type & list_file::kCompressedMask) {
      if (!Uncompress(data_ptr, &length)) {
        wrapper_->ReportCorruption(record_size, "Uncompress failed.");
        return kBadRecord;
      }
      data_ptr = wrapper_->uncompress_buf_.get();
    }

    *result = FromBuf(data_ptr, length);
    return type & 0xF;
  }
}

bool Lst1Impl::Uncompress(const uint8* data_ptr, uint32* size) {
  uint8 method = *data_ptr++;
  VLOG(2) << "Uncompress " << int(method) << " with size " << *size;

  uint32 inp_sz = *size - 1;

  UncompressFunction uncompr_func = GetUncompress(list_file::CompressMethod(method));

  if (!uncompr_func) {
    LOG(ERROR) << "Could not find uncompress method " << int(method);
    return false;
  }
  size_t uncompress_size = wrapper_->block_size;
  Status status = uncompr_func(data_ptr, inp_sz, wrapper_->uncompress_buf_.get(), &uncompress_size);
  if (!status.ok()) {
    VLOG(1) << "Uncompress error: " << status;
    return false;
  }

  *size = uncompress_size;
  return true;
}

const uint8* DecodeString(const uint8* ptr, const uint8* end, string* dest) {
  if (ptr == nullptr)
    return nullptr;
  uint32 string_sz = 0;
  ptr = Varint::Parse32WithLimit(ptr, end, &string_sz);
  if (ptr == nullptr || ptr + string_sz > end)
    return nullptr;
  const char* str = reinterpret_cast<const char*>(ptr);
  dest->assign(str, str + string_sz);
  return ptr + string_sz;
}

}  // namespace

ListReader::FormatImpl::~FormatImpl() {}

ListReader::ReaderWrapper::~ReaderWrapper() {
  if (ownership == TAKE_OWNERSHIP) {
    auto st = file->Close();
    if (!st.ok()) {
      LOG(WARNING) << "Error closing file, status " << st;
    }
    delete file;
  }
}

void ListReader::ReaderWrapper::BadHeader(const Status& st) {
  LOG(ERROR) << "Error reading header " << st;
  ReportDrop(file->Size(), st);
  eof = true;
}

void ListReader::ReaderWrapper::ReportDrop(size_t bytes, const Status& reason) {
  LOG(ERROR) << "ReportDrop: " << bytes << " "
             << " block buffer_size " << block_buffer_.size() << ", reason: " << reason;
  if (reporter_ /*&& end_of_buffer_offset_ >= initial_offset_ + block_buffer_.size() + bytes*/) {
    reporter_(bytes, reason);
  }
}

ListReader::ListReader(file::ReadonlyFile* file, Ownership ownership, bool checksum,
                       CorruptionReporter reporter)
    : wrapper_(new ReaderWrapper(file, ownership, checksum, reporter)) {}

ListReader::ListReader(StringPiece filename, bool checksum, CorruptionReporter reporter) {
  auto res = ReadonlyFile::Open(filename);
  CHECK(res.ok()) << res.status << ", file name: " << filename;
  CHECK(res.obj) << filename;
  wrapper_.reset(new ReaderWrapper(res.obj, TAKE_OWNERSHIP, checksum, reporter));
}

ListReader::~ListReader() {}

bool ListReader::ReadHeader() {
  if (impl_)
    return true;
  if (wrapper_->eof)
    return false;

  static_assert(list_file::kMagicStringSize == lst2::kMagicStringSize, "");

  uint8 buf[list_file::kMagicStringSize] = {0};

  auto res = wrapper_->file->Read(0, strings::MutableByteRange(buf, sizeof(buf)));
  if (!res.ok()) {
    wrapper_->BadHeader(res.status);
    return false;
  }

  const auto read_buf = strings::FromBuf(buf, sizeof(buf));
  const StringPiece kLst1Magic(list_file::kMagicString, list_file::kMagicStringSize);
  if (read_buf == kLst1Magic) {
    impl_.reset(new Lst1Impl(wrapper_.get()));
    return impl_->ReadHeader(&meta_);
  }

  const StringPiece kLst2Magic(lst2::kMagicString, lst2::kMagicStringSize);
  if (read_buf == kLst2Magic) {
    impl_.reset(new lst2::ReaderImpl(wrapper_.get()));
    return impl_->ReadHeader(&meta_);
  }

  wrapper_->BadHeader(Status(StatusCode::PARSE_ERROR, "Invalid header"));
  return false;
}

bool ListReader::GetMetaData(std::map<std::string, std::string>* meta) {
  if (!ReadHeader())
    return false;
  *meta = meta_;
  return true;
}

bool ListReader::ReadRecord(StringPiece* record, std::string* scratch) {
  if (!ReadHeader())
    return false;

  return impl_->ReadRecord(record, scratch);
}

void ListReader::Reset() {
  impl_.reset();
  wrapper_->Reset();
}

Status list_file::HeaderParser::Parse(file::ReadonlyFile* file,
                                      std::map<std::string, std::string>* meta) {
  uint8 buf[2];
  auto res = file->Read(kMagicStringSize, strings::MutableByteRange(buf, 2));
  if (!res.ok())
    return res.status;

  offset_ = kListFileHeaderSize;
  if (buf[0] == 0 || buf[1] > 100) {
    return Status(StatusCode::IO_ERROR, "Invalid header");
  }

  unsigned block_factor = buf[0];
  if (buf[1] == kMetaExtension) {
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

}  // namespace file
