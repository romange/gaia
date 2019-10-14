// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include "absl/container/flat_hash_map.h"
#include "absl/types/optional.h"
#include "mr/mr3.pb.h"

#include "file/file.h"
#include "file/list_file.h"
#include "mr/mr_types.h"
#include "util/fibers/fiberqueue_threadpool.h"

namespace util {
class IoContextPool;
class GCE;

namespace http {
class HttpsClientPool;
}

}  // namespace util

namespace mr3 {
namespace detail {

class DestHandle;

/*! Designed to be process-central data structure holding all the destination handles during
 *  the operator execution.
 */
class DestFileSet {
  const std::string root_dir_;
  const pb::Output& pb_out_;

 public:
  DestFileSet(const std::string& root_dir, const pb::Output& out, util::IoContextPool* pool,
              util::fibers_ext::FiberQueueThreadPool* fq);
  ~DestFileSet();

  //! Closes and deletes all the handles. If abort_write is true, the manager may
  //! delete or drop output files without finalizing them properly.
  //! Useful when we break in the middle of the run.
  void CloseAllHandles(bool abort_write);

  /// Returns full file path of the shard.
  /// if sub_shard is < 0, returns the glob of all files corresponding to this shard.
  std::string ShardFilePath(const ShardId& key, int32 sub_shard) const;

  /// Returns DestHandle that writes into specified shard. The ownership for DestHandle stays
  /// with DestFileSet.
  DestHandle* GetOrCreate(const ShardId& key);

  util::fibers_ext::FiberQueueThreadPool* pool() { return &fq_; }

  std::vector<ShardId> GetShards() const;

  size_t HandleCount() const;

  /// Closes the handle but leaves it in the map.
  /// GatherAll will still return it.
  void CloseHandle(const ShardId& key);

  const pb::Output& output() const { return pb_out_; }

  // HttpsClientPool is thread-local so we need to fetch different instances depending
  // on the thread.
  using PoolAccessorCb = std::function<util::http::HttpsClientPool*()>;
  void set_gce(const util::GCE* gce, PoolAccessorCb pool_accessor) {
    gce_ = gce;
    pool_accessor_ = pool_accessor;
  }

  /// Returns thread local instance of pool managing api connections to GCE.
  util::http::HttpsClientPool* GetGceApiPool() { return pool_accessor_(); }

  const util::GCE* gce() const { return gce_; }

  bool is_gcs_dest() const { return is_gcs_dest_; }

  util::IoContextPool* io_pool() { return &io_pool_; }

 private:
  typedef absl::flat_hash_map<ShardId, std::unique_ptr<DestHandle>> HandleMap;
  HandleMap dest_files_;
  mutable ::boost::fibers::mutex mu_;

  const util::GCE* gce_ = nullptr;
  PoolAccessorCb pool_accessor_;

  util::IoContextPool& io_pool_;
  util::fibers_ext::FiberQueueThreadPool& fq_;
  bool is_gcs_dest_ = false;
};

/*! \class mr3::detail::DestHandle
    \brief Thread-safe handle that abstracts away compression/file formats and disk systems.

    Using Write() function, multiple threads write into the same DestHandle object without
    creating too much contention. The class is especially optimized to work with fibers and
    IOContext threads.
*/
class DestHandle {
  friend class DestFileSet;

  void AppendThreadLocal(const std::string& val);

  static ::file::WriteFile* OpenThreadLocal(const pb::Output& output, const std::string& path);

 public:
  virtual ~DestHandle() {}

  //! Thread-safe. Called from multiple threads/do_contexts.
  //! Implements string generator that allows the writing thread to pull few strings at once.
  using StringGenCb = std::function<absl::optional<std::string>()>;

  //! Writes 0 or many string records according to what cb returns.
  //! When cb is out of records, it will returns absl::nullopt.
  virtual void Write(StringGenCb cb);

  // Thread-safe. Called from multiple threads/do_contexts.
  virtual void Close(bool abort_write);

  void set_raw_limit(size_t raw_limit) { raw_limit_ = raw_limit; }
  const std::string full_path() const { return full_path_; }

 protected:
  template <typename Func> auto Await(Func&& f) {
    return owner_->pool()->Await(queue_index_, std::forward<Func>(f));
  }

  DestHandle(DestFileSet* owner, const ShardId& sid);
  DestHandle(const DestHandle&) = delete;

  virtual void Open();

  DestFileSet* owner_;
  ShardId sid_;

  ::file::WriteFile* write_file_ = nullptr;
  std::string full_path_;

  size_t raw_size_ = 0;
  size_t raw_limit_ = kuint64max;
  uint32_t sub_shard_ = 0;
  uint32_t queue_index_;
};

}  // namespace detail
}  // namespace mr3
