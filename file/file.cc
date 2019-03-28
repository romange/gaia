// Copyright 2012 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Author: tomasz.kaftal@gmail.com (Tomasz Kaftal)
//
// File wrapper implementation.
#include "file/file.h"

#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>
#include <sys/stat.h>
#include <memory>

#include "base/logging.h"
#include "base/macros.h"

using std::string;
using util::Status;
using util::StatusCode;
using util::StatusObject;
using strings::AsString;

namespace file {

Status StatusFileError() {
  char buf[1024];
  char* result = strerror_r(errno, buf, sizeof(buf));

  return Status(StatusCode::IO_ERROR, result);
}

namespace {

static ssize_t read_all(int fd, uint8* buffer, size_t length, size_t offset) {
  size_t left_to_read = length;
  uint8* curr_buf = buffer;
  while (left_to_read > 0) {
    ssize_t read = pread(fd, curr_buf, left_to_read, offset);
    if (read <= 0) {
      return read == 0 ? length - left_to_read : read;
    }

    curr_buf += read;
    offset += read;
    left_to_read -= read;
  }
  return length;
}

// Returns true if a uint64 actually looks like a negative int64. This checks
// if the most significant bit is one.
//
// This function exists because the file interface declares some length/size
// fields to be uint64, and we want to catch the error case where someone
// accidently passes an negative number to one of the interface routines.
inline bool IsUInt64ANegativeInt64(uint64 num) {
  return (static_cast<int64>(num) < 0);
}

// ----------------- LocalFileImpl --------------------------------------------
// Simple file implementation used for local-machine files (mainly temporary)
// only.
class LocalFileImpl : public WriteFile {
 public:

  // flags defined at http://man7.org/linux/man-pages/man2/open.2.html
  LocalFileImpl(StringPiece file_name, int flags) : WriteFile(file_name), flags_(flags) {
  }

  LocalFileImpl(const LocalFileImpl&) = delete;

  virtual ~LocalFileImpl();

  // Return true if file exists.  Returns false if file does not exist or if an
  // error is encountered.
  // virtual bool Exists() const;

  // File handling methods.
  virtual bool Open();
  // virtual bool Delete();
  virtual bool Close();

  Status Write(const uint8* buffer, uint64 length) final;

 protected:
  int fd_ = 0;
  int flags_;
};

LocalFileImpl::~LocalFileImpl() { }

bool LocalFileImpl::Open() {
  if (fd_) {
    LOG(ERROR) << "File already open: " << fd_;
    return false;
  }

  fd_ = open(create_file_name_.c_str(), flags_, 0644);
  if (fd_ < 0) {
    LOG(ERROR) << "Could not open file " << strerror(errno) << " file " << create_file_name_;
    return false;
  }
  return true;
}

bool LocalFileImpl::Close() {
  if (fd_ > 0) {
    close(fd_);
  }
  delete this;
  return true;
}

Status LocalFileImpl::Write(const uint8* buffer, uint64 length) {
  DCHECK(buffer);
  DCHECK(!IsUInt64ANegativeInt64(length));

  uint64 left_to_write = length;
  while (left_to_write > 0) {
    ssize_t written = write(fd_, buffer, left_to_write);
    if (written < 0) {
      return StatusFileError();
    }
    buffer += written;
    left_to_write -= written;
  }

  return Status::OK;
}

}  // namespace

WriteFile::WriteFile(StringPiece name)
    : create_file_name_(AsString(name)) { }

WriteFile::~WriteFile() { }



WriteFile* Open(StringPiece file_name, OpenOptions opts) {
  int flags = O_CREAT | O_WRONLY | O_CLOEXEC;
  if (opts.append)
    flags |= O_APPEND;
  else
    flags |= O_TRUNC;
  WriteFile* ptr = new LocalFileImpl(file_name, flags);
  if (ptr->Open())
    return ptr;
  ptr->Close(); // to delete the object.
  return nullptr;
}

bool Exists(StringPiece fname) {
  return access(fname.data(), F_OK) == 0;
}

bool Delete(StringPiece name) {
  int err;
  if ((err = unlink(name.data())) == 0) {
    return true;
  } else {
    return false;
  }
}

ReadonlyFile::~ReadonlyFile() {
}

// pread() based access.
class PosixReadFile final: public ReadonlyFile {
 private:
  int fd_;
  const size_t file_size_;
  bool drop_cache_;
 public:
  PosixReadFile(int fd, size_t sz, int advice, bool drop) : fd_(fd), file_size_(sz),
          drop_cache_(drop) {
    posix_fadvise(fd_, 0, 0, advice);
  }

  virtual ~PosixReadFile() {
    Close();
  }

  Status Close() override {
    if (fd_) {
      if (drop_cache_)
        posix_fadvise(fd_, 0, 0, POSIX_FADV_DONTNEED);
      close(fd_);
      fd_ = 0;
    }
    return Status::OK;
  }

  StatusObject<size_t> Read(size_t offset, const strings::MutableByteRange& range) override {
    if (range.empty())
      return 0;

    if (offset > file_size_) {
      return Status(StatusCode::RUNTIME_ERROR, "Invalid read range");
    }
    ssize_t r = read_all(fd_, range.begin(), range.size(), offset);
    if (r < 0) {
      return StatusFileError();
    }
    return r;
  }

  size_t Size() const final { return file_size_; }

  int Handle() const final { return fd_; };
};

StatusObject<ReadonlyFile*> ReadonlyFile::Open(StringPiece name, const Options& opts) {
  int fd = open(name.data(), O_RDONLY);
  if (fd < 0) {
    return StatusFileError();
  }
  struct stat sb;
  if (fstat(fd, &sb) < 0) {
    close(fd);
    return StatusFileError();
  }

  int advice = opts.sequential ? POSIX_FADV_SEQUENTIAL : POSIX_FADV_NORMAL;
  return new PosixReadFile(fd, sb.st_size, advice, opts.drop_cache_on_close);
}


} // namespace file

namespace std {

void default_delete<::file::WriteFile>::operator()(::file::WriteFile* ptr) const {
  ptr->Close();
}

}  // namespace std
