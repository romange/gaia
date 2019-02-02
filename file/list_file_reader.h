// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <functional>
#include <map>

#include "base/integral_types.h"
#include "base/logging.h"  // For CHECK.
#include "file/list_file_format.h"
#include "strings/stringpiece.h"
#include "util/status.h"

namespace file {

class ReadonlyFile;

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
  explicit ListReader(StringPiece filename, bool checksum = false, CorruptionReporter = nullptr);

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

  void Reset() { wrapper_->Reset(); }

  uint32_t read_header_bytes() const { return wrapper_->read_header_bytes; }
  uint32_t read_data_bytes() const { return wrapper_->read_data_bytes; }

  class ReaderWrapper {
   public:
    ReaderWrapper(ReadonlyFile* fl, Ownership own, bool chksum, CorruptionReporter rp)
        : file(fl), ownership(own), checksum(chksum), reporter_(rp) {}

    ~ReaderWrapper();

    size_t read_header_bytes = 0;  // how much headers bytes were read so far.
    size_t read_data_bytes = 0;    // how much data bytes were read so far.

    void Reset() {
      block_size = file_offset_ = array_records = 0;
      eof = false;
    }

    void ReportCorruption(size_t bytes, const std::string& reason) {
      ReportDrop(bytes, util::Status(util::StatusCode::IO_ERROR, reason));
    }

    void ReportDrop(size_t bytes, const util::Status& reason);

    ReadonlyFile* file;
    Ownership ownership;
    bool eof = false;  // Last Read() indicated EOF by returning < kBlockSize
    const bool checksum = false;
    uint32_t block_size = 0;
    uint32_t array_records = 0;

    size_t file_offset_ = 0;

    CorruptionReporter const reporter_;
    std::unique_ptr<uint8[]> backing_store_;
    std::unique_ptr<uint8[]> uncompress_buf_;
    strings::ByteRange block_buffer_;

  };

  class FormatImpl {
   public:
    FormatImpl(ReaderWrapper* wrapper) : wrapper_(wrapper) {}
    virtual ~FormatImpl() {}

    virtual bool ReadHeader(std::map<std::string, std::string>* dest) = 0;
    virtual bool ReadRecord(StringPiece* record, std::string* scratch) = 0;

   protected:
    ReaderWrapper* wrapper_;
  };

 private:
  bool ReadHeader();

  std::map<std::string, std::string> meta_;

  std::unique_ptr<ReaderWrapper> wrapper_;
  std::unique_ptr<FormatImpl> impl_;
};

template <typename T> void ReadProtoRecords(ReadonlyFile* file, std::function<void(T&&)> cb) {
  ListReader reader(file, TAKE_OWNERSHIP);
  std::string record_buf;
  StringPiece record;
  while (reader.ReadRecord(&record, &record_buf)) {
    T item;
    CHECK(item.ParseFromArray(record.data(), record.size()));
    cb(std::move(item));
  }
}

template <typename T> void ReadProtoRecords(StringPiece name, std::function<void(T&&)> cb) {
  ListReader reader(name);
  std::string record_buf;
  StringPiece record;
  while (reader.ReadRecord(&record, &record_buf)) {
    typename std::remove_const<T>::type item;
    CHECK(item.ParseFromArray(record.data(), record.size()));
    cb(std::move(item));
  }
}

template <typename MsgT> class PbMsgIter {
 public:
  PbMsgIter() : reader_(nullptr) {}
  explicit PbMsgIter(ListReader* lr) : reader_(lr) { this->operator++(); }

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
  bool operator!=(const PbMsgIter& o) const { return o.reader_ != reader_; }
  operator bool() const { return reader_ != nullptr; }

  const util::Status& status() const { return status_; }

 private:
  MsgT instance_;
  std::string scratch_;
  ListReader* reader_;
  util::Status status_;
};

template <typename T>
util::Status SafeReadProtoRecords(StringPiece name, std::function<void(T&&)> cb) {
  auto res = file::ReadonlyFile::Open(name);
  if (!res.ok()) {
    return res.status;
  }
  CHECK(res.obj);  // Fatal error. If status ok, must contains file pointer.

  file::ListReader reader(res.obj, TAKE_OWNERSHIP);
  PbMsgIter<T> it(&reader);
  while (it) {
    cb(std::move(*it));
    ++it;
  }
  return it.status();
}

}  // namespace file
