// Copyright 2013, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#ifndef FILESOURCE_H
#define FILESOURCE_H

#include <functional>
#include <memory>

#include "base/integral_types.h"
#include "strings/stringpiece.h"
#include "util/sinksource.h"

namespace file {
class ReadonlyFile;
class WriteFile;

class Source : public util::Source {
 public:
  // file must be open for reading.
  Source(ReadonlyFile* file);
  ~Source();


  // Returns the source wrapping the file. If the file is compressed, than the stream
  // automatically inflates the compressed data. The returned source owns the file object.
  static util::Source* Uncompressed(ReadonlyFile* file);
 private:
  util::StatusObject<size_t> ReadInternal(const strings::MutableByteRange& range) override;

  ReadonlyFile* file_;
  uint64 offset_ = 0;
};

class Sink : public util::Sink {
public:
  // file must be open for writing.
  Sink(WriteFile* file, Ownership ownership) : file_(file), ownership_(ownership) {}
  ~Sink();
  util::Status Append(const strings::ByteRange& slice) override;

private:
  WriteFile* file_;
  Ownership ownership_;
};

// Assumes that source provides stream of text characters.
// Will break the stream into lines ending with EOL (either \r\n\ or \n).
class LineReader {
public:
  enum {DEFAULT_BUF_LOG = 17};

  LineReader(util::Source* source, Ownership ownership, uint32_t buf_log = DEFAULT_BUF_LOG)
    : source_(source), ownership_(ownership) {
    Init(buf_log);
  }

  explicit LineReader(const std::string& filename);

  ~LineReader();

  uint64 line_num() const { return line_num_;}

  // Sets the result to point to null-terminated line.
  // Empty lines are also returned.
  // Returns true if new line was found or false if end of stream was reached.
  bool Next(StringPiece* result, std::string* scratch = nullptr);

private:
  void Init(uint32_t buf_log);

  util::Source* source_;
  Ownership ownership_;
  uint64 line_num_ = 0;
  std::unique_ptr<char[]> buf_;
  char* next_, *end_;

  uint32_t page_size_;
  std::string scratch_;
};

class CsvReader {
  LineReader reader_;
  std::function<void(const std::vector<StringPiece>&)> row_cb_;
public:
  explicit CsvReader(const std::string& filename,
                     std::function<void(const std::vector<StringPiece>&)> row_cb);
  void SkipHeader(unsigned rows = 1);

  void Run();
  bool Next(std::vector<StringPiece>* result);

 private:
  std::string scratch_;
  std::vector<char*> parts_;
};

}  // namespace file

#endif  // FILESOURCE_H
