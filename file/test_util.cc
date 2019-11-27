// #include <unistd.h>
#include <sys/stat.h>

#include "base/gtest.h"
#include "base/logging.h"
#include "file/file_util.h"
#include "file/test_util.h"

namespace {

using std::string;
using util::Status;
using util::StatusCode;
using util::StatusObject;

// Creates a temporary directory on demand and deletes it when the process
// quits.
class TempDirDeleter {
 public:
  TempDirDeleter() {
  }
  ~TempDirDeleter() {
    if (!name_.empty()) {
      file_util::DeleteRecursively(name_);
    }
  }

  string GetTempDir() {
    if (name_.empty()) {
      name_ = base::GetTestTempDir();
      CHECK(mkdir(name_.c_str(), 0777) == 0) << strerror(errno);

      // Stick a file in the directory that tells people what this is, in case
      // we abort and don't get a chance to delete it.
      file_util::WriteStringToFileOrDie("", name_ + "/TEMP_DIR_FILE");
    }
    return name_;
  }

 private:
  string name_;
};

TempDirDeleter temp_dir_deleter_;

}  // namespace

string TestTempDir() {
  return temp_dir_deleter_.GetTempDir();
}

namespace file {

StatusObject<size_t> ReadonlyStringFile::Read(size_t offset,
                                              const strings::MutableByteRange& range) {
  if (contents_.size() <= offset)
    return Status(StatusCode::RUNTIME_ERROR, "Beyond string size");
  size_t length = range.size();

  if (contents_.size() < range.size() + offset) {
    length = contents_.size() - offset;
  }

  CHECK(!returned_partial_) << "must not Read() after eof/error";
  if (force_error) {
    force_error = false;
    returned_partial_ = true;
    return Status(StatusCode::IO_ERROR, "read error");
  }

  memcpy(range.begin(), contents_.data() + offset, length);
  return length;
}

RingSource::RingSource(size_t sz, const string& buf) : read_size_(sz), buf_(buf) {
  CHECK_LE(read_size_, buf_.size());
  CHECK(!buf_.empty());
}

StatusObject<size_t> RingSource::ReadInternal(const strings::MutableByteRange& range) {
  size_t sz = std::min(read_size_, range.size());
  sz = std::min(sz, size_t(buf_.size() - index_));

  memcpy(range.data(), buf_.data() + index_, sz);
  index_ += sz;
  if (index_ >= buf_.size()) {
    index_ = 0;
  }
  return sz;
}

}  // namespace file
