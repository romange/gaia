// Copyright 2013, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
// Modified LevelDB implementation of log_format.
#ifndef _LIST_FILE_H_
#define _LIST_FILE_H_

#include <functional>
#include <map>

#include "base/logging.h"   // For CHECK.
#include "file/file.h"
#include "file/compressors.h"
#include "strings/slice.h"
#include "util/sinksource.h"

namespace file {

class ListWriter {
 public:
  struct Options {
    uint8 block_size_multiplier = 1;  // the block size is 64KB * multiplier
    bool use_compression = true;
    list_file::CompressMethod compress_method = list_file::kCompressionLZ4;
    uint8 compress_level = 1;
    bool append = false;

    Options() {}
  };

  // Takes ownership over sink.
  ListWriter(util::Sink* sink, const Options& options = Options());

  // Create a writer that will overwrite filename with data.
  ListWriter(StringPiece filename, const Options& options = Options());
  ~ListWriter();

  // Adds user provided meta information about the file. Must be called before Init.
  void AddMeta(StringPiece key, StringPiece value);

  util::Status Init();
  util::Status AddRecord(StringPiece slice);
  util::Status Flush();

  uint32 records_added() const { return records_added_;}
  uint64 bytes_added() const { return bytes_added_;}
  uint64 compression_savings() const { return compression_savings_;}
 private:

  std::unique_ptr<util::Sink> dest_;
  std::unique_ptr<uint8[]> array_store_;
  std::unique_ptr<uint8[]> compress_buf_;
  std::map<std::string, std::string> meta_;

  uint8* array_next_ = nullptr, *array_end_ = nullptr;  // wraps array_store_
  bool init_called_ = false;

  Options options_;
  uint32 array_records_ = 0;
  uint32 block_offset_ = 0;      // Current offset in block

  uint32 block_size_ = 0;
  uint32 block_leftover_ = 0;
  size_t compress_buf_size_ = 0;

  uint32 records_added_ = 0;
  uint64 bytes_added_ = 0, compression_savings_ = 0;

  void Construct();

  util::Status EmitPhysicalRecord(list_file::RecordType type, const uint8* ptr,
                                  size_t length);

  uint32 block_leftover() const { return block_leftover_; }

  void AddRecordToArray(StringPiece size_enc, StringPiece record);
  util::Status FlushArray();

  CompressFunction compress_func_;

  // No copying allowed
  ListWriter(const ListWriter&) = delete;
  void operator=(const ListWriter&) = delete;
};

class ListReader {
 public:
  // Create a Listreader that will return log records from "*file".
  // "*file" must remain live while this ListReader is in use.
  //
  // If "reporter" is non-NULL, it is notified whenever some data is
  // dropped due to a detected corruption.  "*reporter" must remain
  // live while this ListReader is in use.
  //
  // If "checksum" is true, verify checksums if available.
  //
  // The ListReader will start reading at the first record located at position >= initial_offset
  // relative to start of list records. In afact all positions mentioned in the API are
  // relative to list start position in the file (i.e. file header is read internally and its size
  // is not relevant for the API).
  typedef std::function<void(size_t bytes, const util::Status& status)> CorruptionReporter;

  // initial_offset - file offset AFTER the file header, i.e. offset 0 does not skip anything.
  // File header is read in any case.
  explicit ListReader(ReadonlyFile* file, Ownership ownership, bool checksum = false,
                      CorruptionReporter = nullptr);

  // This version reads the file and owns it.
  explicit ListReader(StringPiece filename, bool checksum = false,
                      CorruptionReporter = nullptr);

  ~ListReader();

  bool GetMetaData(std::map<std::string, std::string>* meta);

  // Read the next record into *record.  Returns true if read
  // successfully, false if we hit end of file. May use
  // "*scratch" as temporary storage.  The contents filled in *record
  // will only be valid until the next mutating operation on this
  // reader or the next mutation to *scratch.
  // If invalid record is encountered, read will continue to the next record and
  // will notify reporter about the corruption.
  bool ReadRecord(StringPiece* record, std::string* scratch);

  // Returns the offset of the last record read by ReadRecord relative to list start position
  // in the file.
  // Undefined before the first call to ReadRecord.
  //size_t LastRecordOffset() const { return last_record_offset_; }

  void Reset() {
    block_size_ = file_offset_ = array_records_ = 0;
    eof_ = false;
  }

  uint32 read_header_bytes() const { return read_header_bytes_;}
  uint32 read_data_bytes() const { return read_data_bytes_; }
private:
  bool ReadHeader();

  // 'size' is size of the compressed blob.
  // Returns true if succeeded. In that case uncompress_buf_ will contain the uncompressed data
  // and size will be updated to the uncompressed size.
  bool Uncompress(const uint8* data_ptr, uint32* size);

  ReadonlyFile* file_;
  size_t file_offset_ = 0;
  size_t read_header_bytes_ = 0;  // how much headers bytes were read so far.
  size_t read_data_bytes_ = 0;  // how much data bytes were read so far.

  Ownership ownership_;
  CorruptionReporter const reporter_;
  bool const checksum_;
  std::unique_ptr<uint8[]> backing_store_;
  std::unique_ptr<uint8[]> uncompress_buf_;
  strings::ByteRange block_buffer_;
  std::map<std::string, std::string> meta_;

  bool eof_ = false;   // Last Read() indicated EOF by returning < kBlockSize

  // Offset of the last record returned by ReadRecord.
  // size_t last_record_offset_;
  // Offset of the first location past the end of buffer_.
  // size_t end_of_buffer_offset_ = 0;

  // Offset at which to start looking for the first record to return
  // size_t const initial_offset_;
  uint32 block_size_ = 0;
  uint32 array_records_ = 0;
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

  // Skips all blocks that are completely before "initial_offset_".
  // util::Status SkipToInitialBlock();

  // Return type, or one of the preceding special values
  unsigned int ReadPhysicalRecord(StringPiece* result);

  // Reports dropped bytes to the reporter.
  // buffer_ must be updated to remove the dropped bytes prior to invocation.
  void ReportCorruption(size_t bytes, const std::string& reason);
  void ReportDrop(size_t bytes, const util::Status& reason);

  // No copying allowed
  ListReader(const ListReader&) = delete;
  void operator=(const ListReader&) = delete;
};

template<typename T> void ReadProtoRecords(ReadonlyFile* file,
                                           std::function<void(T&&)> cb) {
  ListReader reader(file, TAKE_OWNERSHIP);
  std::string record_buf;
  StringPiece record;
  while (reader.ReadRecord(&record, &record_buf)) {
    T item;
    CHECK(item.ParseFromArray(record.data(), record.size()));
    cb(std::move(item));
  }
}

template<typename T> void ReadProtoRecords(StringPiece name,
                                           std::function<void(T&&)> cb) {
  ListReader reader(name);
  std::string record_buf;
  StringPiece record;
  while (reader.ReadRecord(&record, &record_buf)) {
    typename std::remove_const<T>::type item;
    CHECK(item.ParseFromArray(record.data(), record.size()));
    cb(std::move(item));
  }
}




template<typename MsgT> class PbMsgIter {
 public:
  PbMsgIter() : reader_(nullptr) {}
  explicit PbMsgIter(ListReader* lr) : reader_(lr) {
    this->operator++();
  }

  MsgT& operator*() { return instance_; }
  MsgT* operator->() { return &instance_; }

  PbMsgIter<MsgT> begin() { return PbMsgIter<MsgT>(reader_); }
  PbMsgIter<MsgT> end() { return PbMsgIter<MsgT>(); }

  PbMsgIter& operator++() {
    StringPiece record;
    if (!reader_->ReadRecord(&record, &scratch_)) {
      reader_ = nullptr;
    } else {
      if (!instance_.ParseFromArray(record.data(), record.size())) {
        status_.AddErrorMsg("Invalid record");
        reader_ = nullptr;
      }
    }
    return *this;
  }

  // Not really equality. Special casing for sentinel (end()).
  bool operator!=(const PbMsgIter& o) const {
    return o.reader_ != reader_;
  }
  operator bool() const { return reader_ != nullptr; }

  const util::Status& status() const { return status_; }
 private:
  MsgT instance_;
  std::string scratch_;
  ListReader* reader_;
  util::Status status_;
};

template<typename T> util::Status SafeReadProtoRecords(StringPiece name,
                                                      std::function<void(T&&)> cb) {
  auto res = file::ReadonlyFile::Open(name);
  if (!res.ok()) {
    return res.status;
  }
  CHECK(res.obj);   // Fatal error. If status ok, must contains file pointer.

  file::ListReader reader(res.obj, TAKE_OWNERSHIP);
  PbMsgIter<T> it(&reader);
  while (it) {
    cb(std::move(*it));
    ++it;
  }
  return it.status();
}

}  // namespace file

#endif  // _LIST_FILE_H_
