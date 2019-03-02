// Copyright 2012 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
#pragma once

#include <string>

#include "base/integral_types.h"
#include "strings/stringpiece.h"

#include "util/status.h"
#include "base/port.h"

namespace file {

util::Status StatusFileError();

// ReadonlyFile objects are created via ReadonlyFile::Open() factory function
// and are destroyed via "obj->Close(); delete obj;" sequence.
//
class ReadonlyFile {
 protected:
  ReadonlyFile() {}
 public:

  struct Options {
    bool sequential = true;
    bool drop_cache_on_close = true;
    Options()  {}
  };

  virtual ~ReadonlyFile();

  // Reads upto length bytes and updates the result to point to the data.
  // May use buffer for storing data. In case, EOF reached sets result.size() < length but still
  // returns Status::OK.
  virtual util::StatusObject<size_t>
      Read(size_t offset, const strings::MutableByteRange& range) MUST_USE_RESULT = 0;

  // releases the system handle for this file.
  virtual util::Status Close() = 0;

  virtual size_t Size() const = 0;

  // Factory function that creates the ReadonlyFile object.
  // The ownership is passed to the caller.
  static util::StatusObject<ReadonlyFile*>
      Open(StringPiece name, const Options& opts = Options()) MUST_USE_RESULT;
};

// Wrapper class for system functions which handle basic file operations.
// The operations are virtual to enable subclassing, if there is a need for
// different filesystem/file-abstraction support.
class WriteFile {
 public:
  // Flush and Close access to a file handle and delete this File class object.
  // Returns true on success.
  virtual bool Close() = 0;

  // Opens a file. Should not be called directly.
  virtual bool Open() = 0;

  virtual util::Status Write(const uint8* buffer, uint64 length) MUST_USE_RESULT = 0 ;

  util::Status Write(StringPiece slice) MUST_USE_RESULT {
    return Write(reinterpret_cast<const uint8*>(slice.data()), slice.size());
  }

  // Returns the file name given during Create(...) call.
  const std::string& create_file_name() const { return create_file_name_; }

 protected:
  explicit WriteFile(const StringPiece create_file_name);

  // Do *not* call the destructor directly (with the "delete" keyword)
  // nor use scoped_ptr; instead use Close().
  virtual ~WriteFile();

  // Name of the created file.
  const std::string create_file_name_;
};

struct OpenOptions {
  bool append = false;
};

// Factory method to create a new writable file object. Calls Open on the
// resulting object to open the file.
WriteFile* Open(StringPiece file_name, OpenOptions opts = OpenOptions());

// Deletes the file returning true iff successful.
bool Delete(StringPiece name);

bool Exists(StringPiece name);

} // namespace file

namespace std {

template<typename T > struct default_delete;

template <> class default_delete<::file::WriteFile> {
public:
  void operator()(::file::WriteFile* ptr) const;
};

}  // namespace std
