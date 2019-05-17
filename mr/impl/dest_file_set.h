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
  const pb::Output& pb_out_;

  util::fibers_ext::FiberQueueThreadPool* fq_;
  ::boost::fibers::mutex mu_;

 public:
  DestFileSet(const std::string& root_dir, const pb::Output& out,
              util::fibers_ext::FiberQueueThreadPool* fq);
  ~DestFileSet();

  // Closes and deletes all the handles.
  void CloseAllHandles();

  // return full file path of the shard.
  // if sub_shard is < 0, returns the glob of all files corresponding to this shard.
  std::string ShardFilePath(const ShardId& key, int32 sub_shard) const;

  DestHandle* GetOrCreate(const ShardId& key);

  util::fibers_ext::FiberQueueThreadPool* pool() { return fq_; }

  std::vector<ShardId> GetShards() const;

  // Closes the handle but leave it in the map.
  // GatherAll will still return it.
  void CloseHandle(const ShardId& key);

  const pb::Output& output() const { return pb_out_;}

 private:
  typedef absl::flat_hash_map<ShardId, std::unique_ptr<DestHandle>> HandleMap;
  HandleMap dest_files_;
};

class DestHandle {
  friend class DestFileSet;

  void AppendThreadLocal(const std::string& val);
  static ::file::WriteFile* OpenThreadLocal(const pb::Output& output, const std::string& path);

 protected:
  template<typename Func> auto Await(Func&& f) {
    return owner_->pool()->Await(fq_index_, std::forward<Func>(f));
  }

  DestHandle(DestFileSet* owner, const ShardId& sid);
  DestHandle(const DestHandle&) = delete;

  virtual void Open();
 public:
  virtual ~DestHandle() {}

  // Thread-safe. Called from multiple threads/do_contexts.
  virtual void Write(std::string str);

  // Thread-safe. Called from multiple threads/do_contexts.
  virtual void Close();

  void set_raw_limit(size_t raw_limit) { raw_limit_ = raw_limit; }

 protected:
  DestFileSet* owner_;
  ShardId sid_;

  ::file::WriteFile* wf_ = nullptr;
  std::string full_path_;

  size_t raw_size_ = 0;
  size_t raw_limit_ = kuint64max;
  uint32_t sub_shard_ = 0;
  uint32_t fq_index_;
};

class ZlibHandle : public DestHandle {
  friend class DestFileSet;

  ZlibHandle(DestFileSet* owner, const ShardId& sid);

 public:
  void Write(std::string str) override;
  void Close() override;

 private:
  void Open() override;

  size_t start_delta_ = 0;
  util::StringSink* str_sink_ = nullptr;
  std::unique_ptr<util::ZlibSink> zlib_sink_;

  boost::fibers::mutex zmu_;
};

class LstHandle : public DestHandle {
  LstHandle(DestFileSet* owner, const ShardId& sid);

 public:
  void Write(std::string str) override;
  void Close() override;

 private:
  void Open() override;

  std::unique_ptr<file::ListWriter> lst_writer_;
  boost::fibers::mutex mu_;
};

}  // namespace detail
}  // namespace mr3
