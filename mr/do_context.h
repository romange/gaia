// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <string>

#include "mr/mr_types.h"
#include "mr/output.h"

namespace mr3 {

template <typename T> class DoContext;

namespace detail {
template <typename Handler, typename ToType> class HandlerWrapper;
}  // namespace detail

// User facing interfaces
template <typename Record> struct RecordTraits {
  static std::string Serialize(Record&& r) { return std::string(std::move(r)); }

  static bool Parse(std::string&& tmp, Record* res) {
    *res = std::move(tmp);
    return true;
  }
};

// This class is created per IO Context thread. In other words, RawContext is thread-local but
// not fiber local.
class RawContext {
  template <typename T> friend class DoContext;

 public:
  virtual ~RawContext();

  // Flushes pending written data before closing the context. Must be called before destroying
  // the context.
  virtual void Flush() {}

  size_t parse_errors = 0;

  // Returns true if succeeded.
  template <typename Func, typename Val> bool ParseWith(RawRecord&& rr, Func&& f, Val* res) {
    bool parse_res = f(std::move(rr), res);
    if (!parse_res)
      ++parse_errors;

    return parse_res;
  }

 protected:
  virtual void WriteInternal(const ShardId& shard_id, std::string&& record) = 0;
};

// This class is created per MapFiber in SetupDoFn and it wraps RawContext.
template <typename T> class DoContext {
  template <typename Handler, typename ToType> friend class detail::HandlerWrapper;

 public:
  DoContext(const Output<T>& out, RawContext* context) : out_(out), context_(context) {}

  void Write(T&& t) {
    ShardId shard_id = out_.Shard(t);
    std::string dest = rt_.Serialize(std::move(t));
    context_->WriteInternal(shard_id, std::move(dest));
  }

  RawContext* raw_context() { return context_; }

  void SetConstantShard(ShardId sid) { out_.SetConstantShard(sid); }

 private:
  bool ParseRaw(RawRecord&& rr, T* res) {
    return context_->ParseWith(std::move(rr), [this](RawRecord&& rr, T* res) {
      return rt_.Parse(std::move(rr), res);
    }, res);
  }

  Output<T> out_;
  RawContext* context_;
  RecordTraits<T> rt_;
};

}  // namespace mr3
