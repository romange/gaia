// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include "absl/container/flat_hash_map.h"
#include "mr/mr3.pb.h"
#include "mr/mr_types.h"

namespace mr3 {

using ShardFileMap = absl::flat_hash_map<ShardId, std::string>;

class RawContext;

class Runner {
 public:
  virtual ~Runner();

  virtual void Init() = 0;

  virtual void Shutdown() = 0;

  // It's guaranteed that op will live until OperatorEnd is called.
  virtual void OperatorStart(const pb::Operator* op) = 0;

  // Must be thread-safe. Called from multiple threads in operator_executors.
  virtual RawContext* CreateContext() = 0;

  virtual void OperatorEnd(ShardFileMap* out_files) = 0;

  virtual void ExpandGlob(const std::string& glob, std::function<void(const std::string&)> cb) = 0;

  // Read file and fill queue. This function must be fiber-friendly.
  // Returns number of records processed.
  virtual size_t ProcessInputFile(const std::string& filename, pb::WireFormat::Type type,
                                  RawSinkCb cb) = 0;
};

}  // namespace mr3
