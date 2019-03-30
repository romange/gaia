// Copyright 2016, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <functional>
#include <string>
#include "file/file.h"

namespace file_util {

struct StatShort {
  std::string name;
  time_t last_modified;
  off_t size;
  mode_t st_mode;
};

using file::WriteFile;

// Join two path components, adding a slash if necessary.  If basename is an
// absolute path then JoinPath ignores dirname and simply returns basename.
std::string JoinPath(StringPiece dirname, StringPiece basename);

// Retrieve file name from a path. If path dosnt's contain dir returns the path itself.
StringPiece GetNameFromPath(StringPiece path);

StringPiece DirName(StringPiece path);

// Tries to open the file under file_name in the given mode. Will fail if
// there are any errors on the way.
file::WriteFile* OpenOrDie(StringPiece file_name);

// Read an entire file to a std::string.  Return true if successful, false
// otherwise.
bool ReadFileToString(StringPiece name, std::string* output);

// Same as above, but crash on failure.
void ReadFileToStringOrDie(StringPiece name, std::string* output);

// Create a file and write a std::string to it.
void WriteStringToFileOrDie(StringPiece contents, StringPiece name);

// Create a directory.
bool CreateDir(StringPiece name, int mode);

// Create a directory and all parent directories if necessary.
bool RecursivelyCreateDir(StringPiece path, int mode);

// If "name" is a file, we delete it.  If it is a directory, we
// call DeleteRecursively() for each file or directory (other than
// dot and double-dot) within it, and then delete the directory itself.
// The "dummy" parameters have a meaning in the original version of this
// method but they are not used anywhere in protocol buffers.
void DeleteRecursively(StringPiece name);

void TraverseRecursively(StringPiece path, std::function<void(StringPiece)> cb);

int64_t LocalFileSize(StringPiece path);  // In bytes.

// Expands path according to sh rules using expandexp. Does not check the existence of the files.
std::vector<std::string> ExpandPathMultiple(StringPiece path);

// Convenience function similar to ExpandPathMultiple but verifies that the path translates to
// a single result and returns it.
std::string ExpandPath(StringPiece path);

// Uses glob rules for local file system. Returns files that exists.
std::vector<StatShort> StatFiles(StringPiece path);

util::Status StatFilesSafe(StringPiece path, std::vector<StatShort>* res);

// Creates 'file.gz', compresses file, once successful, deletes it. fails on any error.
void CompressToGzip(StringPiece file, uint8_t compress_level = 2);

void CopyFileOrDie(StringPiece src, StringPiece dest_path);

class TempFile {
 public:
  // Creates a file with a temporary-looking filename in read/write
  // mode in directory 'directory_prefix' (which can be followed or
  // not by a "/") or, if 'directory_prefix' is NULL or the empty string,
  // in a temporary directory (as provided by GetExistingTempDirectories()).
  //
  // Returns: a new File*, opened for read/write or NULL if it couldn't create
  // one.
  static file::WriteFile* Create(const char* directory_prefix);

  // The following method returns a temporary-looking filename. Be
  // advised that it might change behavior in the future and the
  // name generated might not follow any specific rule or pattern.
  //
  // Returns a unique filename in directory 'directory_prefix' (which
  // can be followed or not by a "/") or, if 'directory_prefix' is
  // NULL in a local scratch directory. Unique filenames are based on
  // localtime, cycle counts, hostname, pid and tid, to ensure
  // uniqueness. Returns: true if 'filename' contains a unique
  // filename, otherwise false (and 'filename' is left unspecified).
  static bool TempFilename(const char* directory_prefix, std::string* filename);

  // Similar as above but returns the file name rather than writing it to
  // an output argument.
  static std::string TempFilename(const char* directory_prefix);

};

};  // namespace file_util
