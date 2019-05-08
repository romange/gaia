// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include "absl/container/flat_hash_map.h"
#include "mr/mr3.pb.h"

#include "file/file.h"
#include "file/list_file.h"
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
  friend class DestFileSet;

 protected:
  // Does NOT take ownership over wf and fq.
  DestHandle(StringPiece path, ::file::WriteFile* wf, util::fibers_ext::FiberQueueThreadPool* fq);
  DestHandle(const DestHandle&) = delete;

 public:
  virtual ~DestHandle() {}

  virtual void Write(std::string str);
  virtual void Close();

  StringPiece path() const { return full_path_; }

 protected:
  ::file::WriteFile* wf_;
  util::fibers_ext::FiberQueueThreadPool* fq_;
  StringPiece full_path_;
  unsigned fq_index_;
};

class ZlibHandle : public DestHandle {
  friend class DestFileSet;

  ZlibHandle(StringPiece path, const pb::Output& out, ::file::WriteFile* wf,
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
  LstHandle(StringPiece path, ::file::WriteFile* wf, util::fibers_ext::FiberQueueThreadPool* fq);

 public:
  void Write(std::string str) override;
  void Close() override;

 private:
  std::unique_ptr<file::ListWriter> lst_writer_;
  boost::fibers::mutex mu_;
};

}  // namespace detail
}  // namespace mr3
