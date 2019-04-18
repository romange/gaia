// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include "mr/mr3.pb.h"

#include "file/file.h"
#include "mr/mr_types.h"
#include "strings/unique_strings.h"
#include "util/fibers/fiberqueue_threadpool.h"
#include "util/zlib_source.h"

namespace mr3 {
namespace detail {

class DestHandle;

class DestFileSet {
  const std::string root_dir_;

  util::fibers_ext::FiberQueueThreadPool* fq_;
  UniqueStrings str_db_;
  ::boost::fibers::mutex mu_;

 public:
  using Result = DestHandle*;

  DestFileSet(const std::string& root_dir, util::fibers_ext::FiberQueueThreadPool* fq);
  ~DestFileSet();

  void Flush();

  Result Get(const ShardId& key, const pb::Output& out);

  util::fibers_ext::FiberQueueThreadPool* pool() { return fq_; }

  void GatherAll(std::function<void(const ShardId&, DestHandle*)> cb) const;
 private:
  typedef google::dense_hash_map<ShardId, DestHandle*> HandleMap;
  HandleMap dest_files_;
};

class DestHandle {
  ::file::WriteFile* wf_;
  util::StringSink* str_sink = nullptr;
  std::unique_ptr<util::ZlibSink> zlib_sink;
  unsigned fq_index_;
  util::fibers_ext::FiberQueueThreadPool* fq_;
  size_t start_delta_ = 0;

  friend class DestFileSet;

  DestHandle(StringPiece path, ::file::WriteFile* wf, util::fibers_ext::FiberQueueThreadPool* fq);
  DestHandle(const DestHandle&) = delete;

 public:
  void Write(std::string str);
  StringPiece path() const { return full_path_; }

 private:
  StringPiece full_path_;
  boost::fibers::mutex zmu_;
};

}  // namespace detail
}  // namespace mr3
