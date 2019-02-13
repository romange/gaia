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
// Modified by Roman Gershman (romange@gmail.com)
// File management utilities' implementation.

#define __STDC_FORMAT_MACROS 1

#include "file/file_util.h"

#include <dirent.h>
#include <fcntl.h>
#include <glob.h>
#include <stdio.h>
#include <sys/sendfile.h>  // sendfile
#include <sys/stat.h>
#include <zlib.h>

#include <memory>
#include <vector>

#include "base/walltime.h"

#include "absl/strings/match.h"
#include "base/logging.h"
#include "strings/join.h"
#include "strings/stringprintf.h"

using std::vector;
using strings::AsString;
using util::Status;
using util::StatusCode;

namespace file_util {

using file::ReadonlyFile;

static string error_str(int err) {
  string buf(512, '\0');
  char* result = strerror_r(err, &buf.front(), buf.size());
  if (result == buf.c_str())
    return buf;
  else
    return string(result);
}

static void TraverseRecursivelyInternal(StringPiece path, std::function<void(StringPiece)> cb,
                                        uint32 offset) {
  struct stat stats;
  if (stat(path.data(), &stats) != 0) {
    LOG(ERROR) << "TraverseRecursively failed for " << path << "  with error " << error_str(errno);
    return;
  }

  if (S_ISDIR(stats.st_mode)) {
    string current_name;
    DIR* dir = opendir(path.data());
    if (dir == NULL) {
      LOG(ERROR) << "TraverseRecursively: error opening dir " << path
                 << ", error: " << error_str(errno);
      return;
    }

    while (true) {
      struct dirent* der = readdir(dir);
      if (der == nullptr)
        break;
      StringPiece entry_name(der->d_name);
      if (entry_name == StringPiece(".") || entry_name == StringPiece("..")) {
        continue;
      }
      current_name = JoinPath(path, entry_name);
      if (der->d_type == DT_DIR) {
        TraverseRecursivelyInternal(current_name, cb, offset);
      } else {
        StringPiece cn(current_name);
        cn.remove_prefix(offset);
        cb(cn);
      }
    }
    closedir(dir);
  } else if (S_ISREG(stats.st_mode)) {
    path.remove_prefix(offset);
    cb(path);
  } else {
    LOG(WARNING) << "unknown type " << stats.st_mode;
  }
}

inline WriteFile* TryCreate(const char* directory_prefix) {
  // Attempt to create a temporary file.
  string filename;
  if (!TempFile::TempFilename(directory_prefix, &filename))
    return NULL;
  WriteFile* fp = file::Open(filename);
  if (fp) {
    DLOG(INFO) << "Created fname: " << fp->create_file_name();
    return fp;
  }
  return NULL;
}

string JoinPath(StringPiece dirname, StringPiece basename) {
  if ((!basename.empty() && basename[0] == '/') || dirname.empty()) {
    return AsString(basename);
  } else if (dirname[dirname.size() - 1] == '/') {
    return StrCat(dirname, basename);
  } else {
    return StrCat(dirname, "/", basename);
  }
}

StringPiece GetNameFromPath(StringPiece path) {
  size_t file_name_pos = path.rfind('/');
  if (file_name_pos == StringPiece::npos) {
    return path.data();
  }
  return path.data() + file_name_pos + 1;
}

StringPiece DirName(StringPiece path) {
  size_t file_name_pos = path.rfind('/');
  if (file_name_pos == StringPiece::npos) {
    return path;
  }
  return path.substr(0, file_name_pos);
}

WriteFile* OpenOrDie(StringPiece file_name) {
  CHECK(!file_name.empty());
  WriteFile* fp = file::Open(file_name);
  if (fp == NULL) {
    LOG(FATAL) << "Cannot open file " << file_name;
    return NULL;
  }
  return fp;
}

bool ReadFileToString(StringPiece name, string* output) {
  auto res = file::ReadonlyFile::Open(name);
  if (!res.ok())
    return false;

  std::unique_ptr<file::ReadonlyFile> fl(res.obj);

  size_t sz = fl->Size();
  output->resize(sz);

  auto status = fl->Read(0, strings::MutableStringPiece(*output));
  if (!status.ok())
    return false;
  CHECK_EQ(status.obj, sz);
  bool b = fl->Close().ok();

  return b;
}

void ReadFileToStringOrDie(StringPiece name, string* output) {
  CHECK(ReadFileToString(name, output)) << "Could not read: " << name;
}

void WriteStringToFileOrDie(StringPiece contents, StringPiece name) {
  FILE* file = fopen(name.data(), "wb");
  CHECK(file != NULL) << "fopen(" << name << ", \"wb\"): " << strerror(errno);
  CHECK_EQ(fwrite(contents.data(), 1, contents.size(), file), contents.size())
      << "fwrite(" << name << "): " << strerror(errno);
  CHECK(fclose(file) == 0) << "fclose(" << name << "): " << strerror(errno);
}

bool CreateDir(StringPiece name, int mode) { return mkdir(name.data(), mode) == 0; }

bool RecursivelyCreateDir(StringPiece path, int mode) {
  if (CreateDir(path, mode))
    return true;

  if (file::Exists(path))
    return false;

  // Try creating the parent.
  string::size_type slashpos = path.rfind('/');
  if (slashpos == string::npos) {
    // No parent given.
    return false;
  }

  return RecursivelyCreateDir(path.substr(0, slashpos), mode) && CreateDir(path, mode);
}

void DeleteRecursively(StringPiece name) {
  // We don't care too much about error checking here since this is only used
  // in tests to delete temporary directories that are under /tmp anyway.

  // Use opendir()!  Yay!
  // lstat = Don't follow symbolic links.
  struct stat stats;
  if (lstat(name.data(), &stats) != 0)
    return;

  if (S_ISDIR(stats.st_mode)) {
    DIR* dir = opendir(name.data());
    if (dir != NULL) {
      while (true) {
        struct dirent* entry = readdir(dir);
        if (entry == NULL)
          break;
        string entry_name = entry->d_name;
        if (entry_name != "." && entry_name != "..") {
          DeleteRecursively(StrCat(name, "/", entry_name));
        }
      }
    }

    closedir(dir);
    rmdir(name.data());

  } else if (S_ISREG(stats.st_mode)) {
    remove(name.data());
  }
}

void TraverseRecursively(StringPiece path, std::function<void(StringPiece)> cb) {
  CHECK(!path.empty());
  uint32 factor = !absl::EndsWith(path, "/");
  TraverseRecursivelyInternal(path, cb, path.size() + factor);
}

int64_t LocalFileSize(StringPiece path) {
  struct stat statbuf;
  if (stat(path.data(), &statbuf) == -1) {
    return -1;
  }
  return statbuf.st_size;
}

// Tries to create a tempfile in directory 'directory_prefix' or get a
// directory from GetExistingTempDirectories().
/* static */
WriteFile* TempFile::Create(const char* directory_prefix) {
  // If directory_prefix is not provided an already-existing temp directory
  // will be used
  if (!(directory_prefix && *directory_prefix)) {
    return TryCreate(NULL);
  }

  struct stat st;
  if (!(stat(directory_prefix, &st) == 0 && S_ISDIR(st.st_mode))) {
    // Directory_prefix does not point to a directory.
    LOG(ERROR) << "Not a directory: " << directory_prefix;
    return NULL;
  }
  return TryCreate(directory_prefix);
}

// Creates a temporary file name using standard library utilities.
static inline void TempFilenameInDir(const char* directory_prefix, string* filename) {
  int32 tid = static_cast<int32>(pthread_self());
  int32 pid = static_cast<int32>(getpid());
  int64 now_usec = GetCurrentTimeMicros();
  *filename =
      JoinPath(directory_prefix, StringPrintf("tempfile-%x-%d-%" PRId64 "x", tid, pid, now_usec));
}

/* static */
bool TempFile::TempFilename(const char* directory_prefix, string* filename) {
  CHECK(filename != NULL);
  filename->clear();

  if (directory_prefix != NULL) {
    TempFilenameInDir(directory_prefix, filename);
    return true;
  }

  // Re-fetching available dirs ensures thread safety.
  vector<string> dirs;
  // TODO(tkaftal): Namespace might vary depending on glog installation options,
  // I should probably use a configure/make flag here.
  google::GetExistingTempDirectories(&dirs);

  // Try each directory, as they might be full, have inappropriate
  // permissions or have different problems at times.
  for (size_t i = 0; i < dirs.size(); ++i) {
    TempFilenameInDir(dirs[i].c_str(), filename);
    if (file::Exists(*filename)) {
      LOG(WARNING) << "unique tempfile already exists in " << *filename;
      filename->clear();
    } else {
      return true;
    }
  }

  LOG(ERROR) << "Couldn't find a suitable TempFile anywhere. Tried " << dirs.size()
             << " directories";
  return false;
}

/* static */
string TempFile::TempFilename(const char* directory_prefix) {
  string file_name;
  CHECK(TempFilename(directory_prefix, &file_name))
      << "Could not create temporary file with prefix: " << directory_prefix;
  return file_name;
}

// callback funcion for use of glob() at following ExpandFiles() and StatFiles() functions.
int errfunc(const char* epath, int eerrno) {
  LOG(ERROR) << "Error in glob() path: <" << epath << ">. errno: " << eerrno;
  return 0;
}

vector<string> ExpandFiles(StringPiece path) {
  vector<string> res;
  glob_t glob_result;
  int rv = glob(path.data(), 0, errfunc, &glob_result);
  CHECK(!rv || rv == GLOB_NOMATCH) << rv;
  for (size_t i = 0; i < glob_result.gl_pathc; i++) {
    res.push_back(glob_result.gl_pathv[i]);
  }
  globfree(&glob_result);

  return res;
}

// Similar to ExpandFiles but also returns statistics about files sizes and timestamps.
Status StatFilesSafe(StringPiece path, std::vector<StatShort>* res) {
  CHECK_NOTNULL(res);
  glob_t glob_result;
  int rv = glob(path.data(), GLOB_TILDE_CHECK, errfunc, &glob_result);
  if (rv && rv != GLOB_NOMATCH) {
    return Status(StatusCode::IO_ERROR, strerror(rv));
  }

  struct stat statbuf;
  for (size_t i = 0; i < glob_result.gl_pathc; i++) {
    if (stat(glob_result.gl_pathv[i], &statbuf) == 0) {
      StatShort sshort{glob_result.gl_pathv[i], statbuf.st_mtime, statbuf.st_size,
                        statbuf.st_mode};
      res->emplace_back(std::move(sshort));
    } else {
      LOG(WARNING) << "Bad stat for " << glob_result.gl_pathv[i];
    }
  }
  globfree(&glob_result);
  return Status::OK;
}

std::vector<StatShort> StatFiles(StringPiece path) {
  std::vector<StatShort> res;
  Status rv = StatFilesSafe(path, &res);
  CHECK(rv.ok()) << rv;
  return res;
}

void CompressToGzip(StringPiece file, uint8_t compress_level) {
  util::StatusObject<file::ReadonlyFile*> src = file::ReadonlyFile::Open(file);
  CHECK(src.ok()) << file;

  string wfile = StrCat(file, ".gz");

  gzFile dest = gzopen(wfile.c_str(), "wb");
  gzbuffer(dest, 1 << 16);
  gzsetparams(dest, compress_level, Z_DEFAULT_STRATEGY);

  constexpr unsigned kBufSize = 1 << 15;
  std::unique_ptr<uint8[]> buf(new uint8[kBufSize]);
  size_t offs = 0, read = kBufSize;
  std::unique_ptr<ReadonlyFile> src_file(src.obj);
  while (read == kBufSize) {
    auto res = src_file->Read(offs, strings::MutableByteRange(buf.get(), kBufSize));
    CHECK(res.ok()) << res.status;

    read = res.obj;
    if (res.obj) {
      CHECK_GT(gzwrite(dest, buf.get(), read), 0);
    }
    offs += read;
  }
  src_file->Close();
  gzclose_w(dest);
  CHECK(file::Delete(file));
}

void CopyFileOrDie(StringPiece src, StringPiece dest_path) {
  int source = open(src.data(), O_RDONLY, 0);

  CHECK_GT(source, 0);

  // struct required, rationale: function stat() exists also
  struct stat stat_source, stat_dest;
  int err = fstat(source, &stat_source);
  CHECK_EQ(0, err);

  string dest_file;
  if (stat(dest_path.data(), &stat_dest) == 0) {
    if (S_ISDIR(stat_dest.st_mode)) {
      dest_file = JoinPath(dest_path, GetNameFromPath(src));
      dest_path = dest_file.c_str();
    }
  }
  int dest = open(dest_path.data(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  CHECK_GT(dest, 0);

  CHECK_GE(sendfile(dest, source, 0, stat_source.st_size), 0);

  close(source);
  close(dest);
}

}  // namespace file_util
