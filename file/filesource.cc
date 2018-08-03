// Copyright 2013, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "file/filesource.h"

#include "base/logging.h"
#include "file/file.h"
#include "strings/split.h"
#include "strings/strip.h"
#include "util/bzip_source.h"
#include "util/zlib_source.h"
#include "util/zstd_sinksource.h"

namespace file {

// using absl::SkipAs;
using util::Status;
using util::StatusObject;
using namespace std;

Source::Source(ReadonlyFile* file)
 : file_(file) {
}

Source::~Source() {
  CHECK(file_->Close().ok());
  delete file_;
}

util::StatusObject<size_t> Source::ReadInternal(const strings::MutableByteRange& range) {
  auto res = file_->Read(offset_, range);
  if (!res.ok())
    return res.status;
  offset_ += res.obj;

  return res.obj;
}


util::Source* Source::Uncompressed(ReadonlyFile* file) {
  Source* first = new Source(file);
  if (util::ZStdSource::HasValidHeader(first))
    return new util::ZStdSource(first);

  if (util::BzipSource::IsBzipSource(first))
    return new util::BzipSource(first);
  if (util::ZlibSource::IsZlibSource(first))
    return new util::ZlibSource(first);
  return first;
}

Sink::~Sink() {
  if (ownership_ == TAKE_OWNERSHIP)
    CHECK(file_->Close());
}

util::Status Sink::Append(const strings::ByteRange& slice) {
  return file_->Write(slice.data(), slice.size());
}


void LineReader::Init(uint32_t buf_log) {
  CHECK_GT(buf_log, 10);
  page_size_ = 1 << buf_log;

  buf_.reset(new char[page_size_]);
  next_ = end_ = buf_.get();
  *next_ = '\n';
}

LineReader::LineReader(const std::string& fl) : ownership_(TAKE_OWNERSHIP) {
  auto res = ReadonlyFile::Open(fl);
  CHECK(res.ok()) << fl << res.status;

  source_ = file::Source::Uncompressed(res.obj);

  Init(DEFAULT_BUF_LOG);
}

LineReader::~LineReader() {
  if (ownership_ == TAKE_OWNERSHIP) {
    delete source_;
  }
}

bool LineReader::Next(StringPiece* result, std::string* scratch) {
  bool eof = false;
  bool use_scratch = false;

  while (true) {
    // Common case: search of EOL.
    char* ptr = next_;
    while (*ptr != '\n')
      ++ptr;

    if (ptr < end_) {  // Found EOL.
      ++line_num_;

      unsigned delta = 1;
      if (ptr > next_ && ptr[-1] == '\r') {
        --ptr;
        delta = 2;
      }
      *ptr = '\0';

      if (use_scratch) {
        scratch->append(next_, ptr);
        *result = *scratch;
      } else {
        *result = StringPiece(next_, ptr - next_);
      }
      next_ = ptr + delta;

      return true;
    }

    if (end_ != next_) {  // The initial buffer was not empty.
      // We reach end of buffer. We must copy the data to accomodate the broken line.
      if (!use_scratch) {
        if (scratch == nullptr)
          scratch = &scratch_;

        scratch->assign(next_, end_);
        use_scratch = true;
      } else {
        scratch->append(next_, end_);
      }
      next_ = end_;
      if (eof)
        break;
    }

    strings::MutableByteRange range{reinterpret_cast<uint8_t*>(buf_.get()),
                                    /* -1 to allow sentinel */ page_size_ - 1};
    auto s = source_->Read(range);
    if (!s.ok()) {
      return false;
    }

    if (s.obj < page_size_ - 1) {
      eof = true;
      if (s.obj == 0)
        break;
    }
    next_ = buf_.get();
    end_ = next_ + s.obj;
    *end_ = '\n';  // sentinel.
  }

  if (use_scratch) {
    *result = *scratch;
  }
  return next_ != end_;
}

CsvReader::CsvReader(const std::string& filename,
                     std::function<void(const std::vector<StringPiece>&)> row_cb)
    : reader_(filename), row_cb_(row_cb) {
}

void CsvReader::SkipHeader(unsigned rows) {
  string tmp;
  StringPiece tmp2;
  for (unsigned i = 0; i < rows; ++i) {
    if (!reader_.Next(&tmp2, &tmp))
      return;
  }
}

bool CsvReader::Next(std::vector<StringPiece>* result) {
  StringPiece line;

  while (true) {
    if (!reader_.Next(&line, &scratch_))
      return false;
    line = absl::StripAsciiWhitespace(line);
    if (line.empty())
      continue;
    char* ptr = const_cast<char*>(line.data());
    parts_.clear();
    SplitCSVLineWithDelimiter(ptr, ',',  &parts_);
    result->assign(parts_.begin(), parts_.end());

    return true;
  }
  return false;
}

void CsvReader::Run() {
  vector<StringPiece> result;

  while (Next(&result)) {
    row_cb_(result);
  }
}

}  // namespace file
