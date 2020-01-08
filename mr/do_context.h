// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <string>

#include "absl/container/flat_hash_map.h"

#include "mr/impl/freq_map_wrapper.h"
#include "mr/mr_types.h"
#include "mr/output.h"
#include "strings/unique_strings.h"

namespace mr3 {

template <typename T> class DoContext;
class OperatorExecutor;

namespace detail {
template <typename Handler, typename ToType> class HandlerWrapper;

void VerifyUnspecifiedSharding(const pb::Output& outp);

}  // namespace detail

// User facing interfaces. void tag for dispatching per class of types
// (i.e. derived from protobuf::Message etc).
// TODO: this design is not composable.
// i.e. I would like to be able to define serializers for basic types and easily compose more
// complicated ones.
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

/** RawContext and its wrapper DoContext<T> provide bidirectional interface from user classes
 *  to the framework.
 *  RawContextis created per IO Context thread. In other words, RawContext is thread-local but
 *  not necessarily fiber local.
 */
class RawContext {
  template <typename T> friend class DoContext;
  friend class OperatorExecutor;
 public:
  //! std/absl monostate is an empty class that gives variant optional semantics.
  using InputMetaData = absl::variant<absl::monostate, int64_t, std::string>;
  using FreqMapRegistry =
    absl::flat_hash_map<std::string, detail::FreqMapWrapper>;

  RawContext();

  virtual ~RawContext();

  /// Flushes pending written data before closing the context. Must be called before destroying
  /// the context.
  virtual void Flush() {}
  virtual void CloseShard(const ShardId& sid) = 0;

  //! MR metrics - are used for monitoring, exposing statistics via http
  void IncBy(StringPiece name, long delta) { metric_map_[name] += delta; }
  void Inc(StringPiece name) { IncBy(name, 1); }
  StringPieceDenseMap<long>& metric_map() { return metric_map_; }

  /// Called for every IO thread in order to fetch the metric map parts from all of them,
  /// updates into metric_map_.
  void UpdateMetricMap(MetricMap* metric_map) {
    for (const auto& k_v : metric_map_)
      (*metric_map)[std::string(k_v.first)] += k_v.second;
  }

  // Used only in tests.
  void TEST_Write(const ShardId& shard_id, std::string&& record) {
    Write(shard_id, std::move(record));
  }

  void EmitParseError() { ++metric_map_["parse-errors"]; }

  const std::string& input_file_name() const { return file_name_;}

  // Line number for text files.
  size_t input_pos() const { return input_pos_; }

  const InputMetaData& meta_data() const { return metadata_;}
  bool is_binary() const { return is_binary_; }

  template <class T>
  FrequencyMap<T>&  GetFreqMapStatistic(const std::string& map_id) {
    auto res = freq_maps_.emplace(map_id, detail::FreqMapWrapper());
    if (res.second) {
      res.first->second = detail::FreqMapWrapper(FrequencyMap<T>());
    }
    return res.first->second.Cast<T>();
  }

  // Finds the map produced by operators in the previous steps
  template <class T>
  const FrequencyMap<T>* FindMaterializedFreqMapStatistic(
      const std::string& map_id) const {
    const detail::FreqMapWrapper *ptr = FindMaterializedFreqMapStatisticImpl(map_id);
    return &ptr->Cast<T>();
  }

  const ShardId& current_shard() const { return current_shard_;}

 private:
  void Write(const ShardId& shard_id, std::string&& record) {
    ++metric_map_["fn-writes"];
    WriteInternal(shard_id, std::move(record));
  }

  const detail::FreqMapWrapper *FindMaterializedFreqMapStatisticImpl(const std::string&) const;

  // To allow testing we mark this function as public.
  virtual void WriteInternal(const ShardId& shard_id, std::string&& record) = 0;

  StringPieceDenseMap<long> metric_map_;
  std::string file_name_;
  ShardId current_shard_;

  InputMetaData metadata_;
  bool is_binary_ = false;

  FreqMapRegistry freq_maps_;
  const FreqMapRegistry* finalized_maps_ = nullptr;
  size_t input_pos_ = 0;
};

// This class is created per MapFiber in SetupDoFn and it wraps RawContext.
// It's thread-local.
template <typename T> class DoContext {
  template <typename Handler, typename ToType> friend class detail::HandlerWrapper;

 public:
  DoContext(const Output<T>& out, RawContext* context) : out_(out), context_(context) {}

  template<typename U> void Write(const ShardId& shard_id, U&& u) {
    context_->Write(shard_id, rt_.Serialize(out_.is_binary(), std::forward<U>(u)));
  }

  void Write(T& t) {
    ShardId shard_id = out_.Shard(t);
    Write(shard_id, t);
  }

  void Write(T&& t) {
    ShardId shard_id = out_.Shard(t);
    Write(shard_id, std::move(t));
  }

  RawContext* raw() { return context_; }

  //
  void SetOutputShard(ShardId sid) {
    detail::VerifyUnspecifiedSharding(out_.msg());
    out_.SetConstantShard(std::move(sid));
  }

  void CloseShard(const ShardId& sid) { raw()->CloseShard(sid); }

private:

  Output<T> out_;
  RawContext* context_;
  RecordTraits<T> rt_;
};

}  // namespace mr3
