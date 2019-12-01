// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <string>

#include "absl/container/flat_hash_map.h"
#include "absl/types/any.h"

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

template<typename T> using FrequencyMap = absl::flat_hash_map<T, size_t>;

class AnyFreqMap {
public:
  template <class T>
  AnyFreqMap(FrequencyMap<T>&& m) : any_(std::move(m)) {}

  AnyFreqMap() {}

  bool has_value() const { return any_.has_value(); }
  std::type_info type() const;
  void Add(const AnyFreqMap& other) { extra_functions_->Add(other, this); }
  template <class T> FrequencyMap<T>& Cast() {
    CheckType(typeid(FrequencyMap<T>));
    return *absl::any_cast<FrequencyMap<T>>(&any_);
  }
  template <class T> const FrequencyMap<T>& Cast() const {
    CheckType(typeid(FrequencyMap<T>));
    return *absl::any_cast<FrequencyMap<T>>(&any_);
  }

private:
  class ExtraFunctions {
  public:
    virtual void Add(const AnyFreqMap& other, AnyFreqMap *that) = 0;
    virtual ~ExtraFunctions() {}
  };

  template <class T>
  class ExtraFunctionsImpl {
  public:
    void Add(const AnyFreqMap& other, AnyFreqMap *that) override {
      for (auto& map_value : other.Cast<T>())
        (that->Cast<T>())[map_value.first] += map_value.second;
    }
  };

  void CheckType(const std::type_info&) const;

  absl::any any_;
  std::unique_ptr<ExtraFunctions> extra_functions_;
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
    absl::flat_hash_map<std::string, AnyFreqMap>;

  RawContext();

  virtual ~RawContext();

  /// Flushes pending written data before closing the context. Must be called before destroying
  /// the context.
  virtual void Flush() {}
  virtual void CloseShard(const ShardId& sid) = 0;

  //! MR metrics - are used for monitoring, exposing statistics via http
  void IncBy(StringPiece name, long delta) { metric_map_[name] += delta; }
  void Inc(StringPiece name) { IncBy(name, 1); }
  // const StringPieceDenseMap<long>& metric_map() const { return metric_map_; }

  // Used only in tests.
  void TEST_Write(const ShardId& shard_id, std::string&& record) {
    Write(shard_id, std::move(record));
  }

  void EmitParseError() { ++parse_errors_; }

  size_t parse_errors() const { return parse_errors_;}
  size_t item_writes() const { return item_writes_;}

  const std::string& input_file_name() const { return file_name_;}

  // Line number for text files.
  size_t input_pos() const { return input_pos_; }

  const InputMetaData& meta_data() const { return metadata_;}
  bool is_binary() const { return is_binary_; }

  //! TODO: to make GetMutableMap templated to support various keys.
  //! map_id must be unique for each map across the whole pipeline run.
  template <class T>
  FrequencyMap<T>&  GetFreqMapStatistic(const std::string& map_id) {
    auto res = freq_maps_.emplace(map_id, AnyFreqMap());
    if (res.second) {
      res.first->second = AnyFreqMap(FrequencyMap<T>());
    }
    return res.first->second.Cast<T>();
  }

  // Finds the map produced by operators in the previous steps
  template <class T>
  const FrequencyMap<T>* FindMaterializedFreqMapStatistic(
      const std::string& map_id) const {
    const AnyFreqMap *ptr = FindMaterializedFreqMapStatisticNotNull(map_id);
    return ptr->Cast<T>();
  }

  const ShardId& current_shard() const { return current_shard_;}

 private:
  void Write(const ShardId& shard_id, std::string&& record) {
    ++item_writes_;
    WriteInternal(shard_id, std::move(record));
  }

  const AnyFreqMap *FindMaterializedFreqMapStatisticNotNull(const std::string&) const;

  // To allow testing we mark this function as public.
  virtual void WriteInternal(const ShardId& shard_id, std::string&& record) = 0;

  StringPieceDenseMap<long> metric_map_;
  size_t parse_errors_ = 0, item_writes_ = 0;
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
