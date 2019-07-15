// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include "base/type_traits.h"

#include "mr/mr3.pb.h"
#include "mr/mr_types.h"

namespace mr3 {

namespace detail {
  template <typename OutT> class TableImplT;
  inline bool IsBinary(pb::WireFormat::Type tp) { return tp == pb::WireFormat::LST; }
}

class OutputBase {
 public:
  pb::Output* mutable_msg() { return out_; }
  const pb::Output& msg() const { return *out_; }

  bool is_binary() const { return detail::IsBinary(out_->format().type()); }

 protected:
  pb::Output* out_;

  OutputBase(pb::Output* out) : out_(out) {}

  void SetCompress(pb::Output::CompressType ct, unsigned level);
  void SetShardSpec(pb::ShardSpec::Type st, unsigned modn = 0);
};

template <typename T> class Output : public OutputBase {
  friend class detail::TableImplT<T>;  // To allow the instantiation of Output<T>;

  using CustomShardingFunc = std::function<std::string(const T&)>;
  using ModNShardingFunc = std::function<unsigned(const T&)>;

  absl::variant<absl::monostate, ShardId, ModNShardingFunc, CustomShardingFunc> shard_op_;
  unsigned modn_ = 0;

  struct Visitor {
    const T& t_;
    unsigned modn_;

    Visitor(const T& t, unsigned modn) :t_(t), modn_(modn) {}

    ShardId operator()(const ShardId& id) const { return id; }
    ShardId operator()(const ModNShardingFunc& func) const { return ShardId{func(t_) % modn_}; }
    ShardId operator()(const CustomShardingFunc& func) const { return ShardId{func(t_)}; }
    ShardId operator()(absl::monostate ms) const { return ms; }
  };

 public:
  Output() : OutputBase(nullptr) {}

  template <typename U> Output& WithCustomSharding(U&& func) {
    static_assert(base::is_invocable_r<std::string, U, const T&>::value, "");
    shard_op_ = std::forward<U>(func);
    SetShardSpec(pb::ShardSpec::USER_DEFINED);

    return *this;
  }

  template <typename U> Output& WithModNSharding(unsigned modn, U&& func) {
    static_assert(base::is_invocable_r<unsigned, U, const T&>::value, "");
    shard_op_ = std::forward<U>(func);
    SetShardSpec(pb::ShardSpec::MODN, modn);
    modn_ = modn;

    return *this;
  }

  Output& AndCompress(pb::Output::CompressType ct, unsigned level = 0);

  ShardId Shard(const T& t) const {
    return absl::visit(Visitor{t, modn_}, shard_op_);
  }


  // TODO: to expose it for friends.
  void SetConstantShard(ShardId sid) { shard_op_ = std::move(sid); }

 private:
  Output(pb::Output* out) : OutputBase(out) {}
};

template <typename OutT>
Output<OutT>& Output<OutT>::AndCompress(pb::Output::CompressType ct, unsigned level) {
  SetCompress(ct, level);
  return *this;
}

}  // namespace mr3
