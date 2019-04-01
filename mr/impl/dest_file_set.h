// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include "mr/mr3.pb.h"

#include "file/file.h"
#include "strings/unique_strings.h"
#include "util/fibers/fiberqueue_threadpool.h"
#include "util/fibers/fibers_ext.h"
#include "util/sinksource.h"
#include "util/zlib_source.h"

namespace mr3 {
namespace impl {

class DestHandle;

class DestFileSet {
  const std::string root_dir_;

  util::fibers_ext::FiberQueueThreadPool* fq_;
  typedef google::dense_hash_map<StringPiece, DestHandle*> HandleMap;

  HandleMap dest_files_;
  UniqueStrings str_db_;
  ::boost::fibers::mutex mu_;

 public:
  using Result = std::pair<StringPiece, DestHandle*>;

  DestFileSet(const std::string& root_dir, util::fibers_ext::FiberQueueThreadPool* fq);
  ~DestFileSet();

  void Flush();

  Result Get(StringPiece key, const pb::Output& out);

  util::fibers_ext::FiberQueueThreadPool* pool() { return fq_; }
};

class DestHandle {
  ::file::WriteFile* wf_;
  util::StringSink* str_sink = nullptr;
  std::unique_ptr<util::ZlibSink> zlib_sink;
  unsigned fq_index_;
  util::fibers_ext::FiberQueueThreadPool* fq_;

  friend class DestFileSet;

  DestHandle(::file::WriteFile* wf, unsigned index, util::fibers_ext::FiberQueueThreadPool* fq);
  DestHandle(const DestHandle&) = delete;

 public:
  void Write(std::string str);

 private:
  boost::fibers::mutex zmu_;
};

}  // namespace impl
}  // namespace mr3
