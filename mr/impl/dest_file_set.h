// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include "absl/container/flat_hash_map.h"
#include "mr/mr3.pb.h"

#include "file/file.h"
#include "file/list_file.h"
#include "mr/mr_types.h"
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
  ::boost::fibers::mutex mu_;

 public:
  using Result = DestHandle*;

  DestFileSet(const std::string& root_dir, util::fibers_ext::FiberQueueThreadPool* fq);
  ~DestFileSet();

  // Closes and deletes all the handles.
  void CloseAllHandles();

  // return full file path of the shard.
  // if sub_shard is < 0, returns the glob of all files corresponding to this shard.
  std::string ShardFilePath(const ShardId& key, const pb::Output& out, int32 sub_shard) const;

  Result GetOrCreate(const ShardId& key, const pb::Output& out);

  util::fibers_ext::FiberQueueThreadPool* pool() { return fq_; }

  std::vector<ShardId> GetShards() const;

  // Closes the handle but leave it in the map.
  // GatherAll will still return it.
  void CloseHandle(const ShardId& key);

 private:
  typedef absl::flat_hash_map<ShardId, std::unique_ptr<DestHandle>> HandleMap;
  HandleMap dest_files_;
};

class DestHandle {
  friend class DestFileSet;

 protected:
  // Does NOT take ownership over wf and fq.
  DestHandle(const std::string& path, ::file::WriteFile* wf,
             util::fibers_ext::FiberQueueThreadPool* fq);
  DestHandle(const DestHandle&) = delete;

 public:
  virtual ~DestHandle() {}

  virtual void Write(std::string str);
  virtual void Close();

  const std::string& path() const { return full_path_; }

 protected:
  ::file::WriteFile* wf_;
  util::fibers_ext::FiberQueueThreadPool* fq_;
  std::string full_path_;
  unsigned fq_index_;
};

class ZlibHandle : public DestHandle {
  friend class DestFileSet;

  ZlibHandle(const std::string& path, const pb::Output& out, ::file::WriteFile* wf,
             util::fibers_ext::FiberQueueThreadPool* fq);

 public:
  void Write(std::string str) override;
  void Close() override;

 private:
  size_t start_delta_ = 0;
  util::StringSink* str_sink_ = nullptr;
  std::unique_ptr<util::ZlibSink> zlib_sink_;

  boost::fibers::mutex zmu_;
};

class LstHandle : public DestHandle {
  LstHandle(const std::string& path, ::file::WriteFile* wf,
            util::fibers_ext::FiberQueueThreadPool* fq);

 public:
  void Write(std::string str) override;
  void Close() override;

 private:
  std::unique_ptr<file::ListWriter> lst_writer_;
  boost::fibers::mutex mu_;
};

}  // namespace detail
}  // namespace mr3
