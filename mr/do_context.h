// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <string>

#include "mr/mr_types.h"
#include "mr/output.h"
#include "strings/unique_strings.h"

namespace mr3 {

template <typename T> class DoContext;

namespace detail {
template <typename Handler, typename ToType> class HandlerWrapper;
}  // namespace detail

// User facing interfaces. void tag for dispatching per class of types
// (i.e. derived from protobuf::Message etc).
template <typename Record, typename = void> struct RecordTraits {
  static_assert(sizeof(base::void_t<Record>) == 0, "Please specify RecordTraits<> for this type");
};

template <> struct RecordTraits<std::string> {
  static std::string Serialize(bool is_binary, std::string&& r) { return std::move(r); }

  static bool Parse(bool is_binary, std::string&& tmp, std::string* res) {
    *res = std::move(tmp);
    return true;
  }
};

// This class is created per IO Context thread. In other words, RawContext is thread-local but
// not fiber local.
class RawContext {
  template <typename T> friend class DoContext;

 public:
  RawContext();

  virtual ~RawContext();

  // Flushes pending written data before closing the context. Must be called before destroying
  // the context.
  virtual void Flush() {}
  virtual void CloseShard(const ShardId& sid) = 0;

  size_t parse_errors = 0;

  void IncBy(StringPiece name, long delta) { counter_map_[name] += delta; }

  void Inc(StringPiece name) { IncBy(name, 1); }

  const StringPieceDenseMap<long>& counter_map() const { return counter_map_; }

  // Used only in tests.
  void TEST_Write(const ShardId& shard_id, std::string&& record) {
    WriteInternal(shard_id, std::move(record));
  }

 private:

  // To allow testing we mark this function as public.
  virtual void WriteInternal(const ShardId& shard_id, std::string&& record) = 0;

  StringPieceDenseMap<long> counter_map_;
};

// This class is created per MapFiber in SetupDoFn and it wraps RawContext.
// It's thread-local.
template <typename T> class DoContext {
  template <typename Handler, typename ToType> friend class detail::HandlerWrapper;

 public:
  DoContext(const Output<T>& out, RawContext* context) : out_(out), context_(context) {}

  void Write(T&& t) {
    ShardId shard_id = out_.Shard(t);
    std::string dest = rt_.Serialize(out_.is_binary(), std::move(t));
    context_->WriteInternal(shard_id, std::move(dest));
  }

  RawContext* raw() { return context_; }

  void SetConstantShard(ShardId sid) { out_.SetConstantShard(sid); }

 private:
  Output<T> out_;
  RawContext* context_;
  RecordTraits<T> rt_;
};

}  // namespace mr3
