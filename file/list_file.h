// Copyright 2013, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
// Modified LevelDB implementation of log_format.
#ifndef _LIST_FILE_H_
#define _LIST_FILE_H_

#include <functional>
#include <map>

#include "file/list_file_format.h"
#include "file/file.h"
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
    bool v2 = false;

    Options() {}

    size_t internal_append_offset = 0;
  };

  // Takes ownership over sink.
  ListWriter(util::Sink* sink, const Options& options = Options());

  // Create a writer that will overwrite filename with data.
  ListWriter(StringPiece filename, const Options& options = Options());

  // Adds user provided meta information about the file. Must be called before Init.
  void AddMeta(StringPiece key, StringPiece value);

  util::Status Init() { return impl_->Init(meta_); }

  util::Status AddRecord(StringPiece slice) { return impl_->AddRecord(slice); }

  util::Status Flush() { return impl_->Flush(); }

  uint32 records_added() const { return impl_->records_added(); }
  uint64 bytes_added() const { return impl_->bytes_added(); }
  uint64 compression_savings() const { return impl_->compression_savings(); }

  class WriterImpl {
   public:
    explicit WriterImpl(util::Sink* dest, const Options& opts) : dest_(dest), options_(opts) {}

    virtual ~WriterImpl() {}

    virtual util::Status Init(const std::map<std::string, std::string>& meta) = 0;
    virtual util::Status AddRecord(StringPiece slice) = 0;
    virtual util::Status Flush() = 0;

    uint32 records_added() const { return records_added_; }
    uint64 bytes_added() const { return bytes_added_; }
    uint64 compression_savings() const { return compression_savings_; }

    bool init_called() const { return init_called_; }
    const Options& options() const { return options_; }

   protected:

    std::unique_ptr<util::Sink> dest_;
    bool init_called_ = false;
    uint32 records_added_ = 0;
    uint64 bytes_added_ = 0, compression_savings_ = 0;
    Options options_;
  };

 private:
  std::unique_ptr<WriterImpl> impl_;
  std::map<std::string, std::string> meta_;
};

}  // namespace file

#include "file/list_file_reader.h"

#endif  // _LIST_FILE_H_
