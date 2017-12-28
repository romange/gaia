// Copyright 2015, .com .  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "file/lz4_file.h"

#include "base/logging.h"
#include <lz4frame.h>
#include <fcntl.h>

using base::Status;
using base::StatusCode;

constexpr size_t kInputBatchSize = 1 << 16;

namespace file {

LZ4File::LZ4File(StringPiece file_name, unsigned level) : WriteFile(file_name), level_(level) {
}

LZ4File::~LZ4File() {
  delete[] buf_;
}

bool LZ4File::Open() {
  CHECK_EQ(0, fd_);
  int flags = O_CREAT | O_WRONLY | O_CLOEXEC | O_TRUNC;
  fd_ = open(create_file_name_.c_str(), flags, 0666);
  if (fd_ < 0) {
    LOG(ERROR) << "Could not open file " << strerror(errno);
    return false;
  }

  LZ4F_preferences_t prefs;
  LZ4F_errorCode_t result = LZ4F_createCompressionContext(&context_, LZ4F_VERSION);
  CHECK(!LZ4F_isError(result)) << LZ4F_getErrorName(result);

  /* Init */
  memset(&prefs, 0, sizeof(prefs));

  prefs.autoFlush = 1;
  prefs.compressionLevel = level_;
  prefs.frameInfo.blockSizeID = LZ4F_max64KB;
  char dst_buf[32];

  size_t header_size = LZ4F_compressBegin(context_, dst_buf, sizeof(dst_buf), &prefs);
  CHECK(!LZ4F_isError(header_size)) << LZ4F_getErrorName(header_size);

  ssize_t sz = write(fd_, dst_buf, header_size);
  if (sz < 0) {
    LOG(ERROR) << "Could not open file " << strerror(errno);
    LZ4F_freeCompressionContext(context_);
    close(fd_);
    fd_ = 0;
    return false;
  }

  CHECK_EQ(header_size, sz);
  buf_size_ = LZ4F_compressBound(kInputBatchSize, &prefs);

  buf_ = new uint8[buf_size_];
  return true;
}

bool LZ4File::Close() {
  if (fd_ == 0) return true;

  size_t header_size = LZ4F_compressEnd(context_, buf_, buf_size_, NULL);
  if (LZ4F_isError(header_size)) {
    LOG(ERROR) << "Error closing file " << LZ4F_getErrorName(header_size);
    return false;
  }

  ssize_t sz = write(fd_, buf_, header_size);
  if (sz < 0) {
    LOG(ERROR) << "Error closing file " << strerror(errno);
    return false;
  }
  close(fd_);
  LZ4F_freeCompressionContext(context_);

  delete this;

  return true;
}

Status LZ4File::Write(const uint8* buffer, uint64 length) {
  do {
    size_t bl = std::min(length, kInputBatchSize);
    size_t written = LZ4F_compressUpdate(context_, buf_, buf_size_, buffer, bl, NULL);
    if (LZ4F_isError(written)) {
      return Status(LZ4F_getErrorName(written));
    }
    buffer += bl;
    length -= bl;
    if (written) {
      ssize_t sz = write(fd_, buf_, written);
      if (sz < 0) {
        return StatusFileError();
      }
    }

  } while (length);
  return Status::OK;
}

}  // namespace file
