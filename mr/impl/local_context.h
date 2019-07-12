// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include "mr/do_context.h"
#include "mr/impl/dest_file_set.h"

namespace mr3 {

namespace detail {

class BufferedWriter;

class LocalContext : public RawContext {
 public:
  explicit LocalContext(DestFileSet* mgr);
  ~LocalContext();

  void Flush() final;

  void CloseShard(const ShardId& sid) final;

 private:
  void WriteInternal(const ShardId& shard_id, std::string&& record) final;

  absl::flat_hash_map<ShardId, BufferedWriter*> custom_shard_files_;

  DestFileSet* mgr_;
};

}  // namespace detail
}  // namespace mr3
