// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include "absl/container/flat_hash_map.h"
#include "mr/mr3.pb.h"

#include "file/file.h"
#include "mr/mr_types.h"
#include "strings/unique_strings.h"
#include "util/fibers/fiberqueue_threadpool.h"
#include "util/zlib_source.h"

namespace mr3 {
namespace detail {

class DestHandle;

// designed to be process central data structure holding all the destination files during
// the operator execution.
class DestFileSet {
  const std::string root_dir_;

  util::fibers_ext::FiberQueueThreadPool* fq_;
  UniqueStrings str_db_;
  ::boost::fibers::mutex mu_;

 public:
  using Result = DestHandle*;

  DestFileSet(const std::string& root_dir, util::fibers_ext::FiberQueueThreadPool* fq);
  ~DestFileSet();

  // Closes and deletes all the handles.
  void CloseAllHandles();

  Result GetOrCreate(const ShardId& key, const pb::Output& out);

  util::fibers_ext::FiberQueueThreadPool* pool() { return fq_; }

  void GatherAll(std::function<void(const ShardId&, DestHandle*)> cb) const;

  // Closes the handle but leave it in the map.
  // GatherAll will still return it. 
  void CloseHandle(const ShardId& key);

 private:
  typedef absl::flat_hash_map<ShardId, std::unique_ptr<DestHandle>> HandleMap;
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
  void Close();

 private:
  StringPiece full_path_;
  boost::fibers::mutex zmu_;
};

}  // namespace detail
}  // namespace mr3
