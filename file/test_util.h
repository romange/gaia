#ifndef TEST_UTIL_H
#define TEST_UTIL_H

#include <string>
#include "file/file.h"

// When running unittests, get the directory containing the source code.
std::string TestSourceDir();

// When running unittests, get a directory where temporary files may be
// placed.
std::string TestTempDir();

namespace file {


class NullFile : public WriteFile {
 public:
  NullFile() : WriteFile("NullFile") {}
  virtual bool Close() override { return true; }
  virtual bool Open() override { return true; }

  util::Status Write(const uint8* ,uint64) override { return util::Status::OK; }
};

class ReadonlyStringFile : public ReadonlyFile {
  std::string contents_;
public:
  ReadonlyStringFile(std::string str = std::string()) : contents_(std::move(str)) {}

  // Reads upto length bytes and updates the result to point to the data.
  // May use buffer for storing data.
  util::StatusObject<size_t> Read(size_t offset, const strings::MutableByteRange& range) override;

  // releases the system handle for this file.
  util::Status Close() override { return util::Status::OK; }

  size_t Size() const override { return contents_.size(); }

  bool force_error = false;

  void set_contents(const std::string& c) { contents_ = c; }

   int Handle() const { return 0; }
 private:
  bool returned_partial_ = false;
};

}  // namespace file

#endif  // TEST_UTIL_H
