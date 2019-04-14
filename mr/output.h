// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include "absl/strings/string_view.h"
#include "absl/types/variant.h"
#include "base/type_traits.h"
#include "mr/mr3.pb.h"

namespace mr3 {

namespace detail {
template <typename OutT> class TableImpl;
}  // detail

struct ShardId : public absl::variant<uint32_t, std::string> {
  using Parent = absl::variant<uint32_t, std::string>;

  using Parent::Parent;

  ShardId() = default;

  std::string ToString(absl::string_view basename) const;
};

class OutputBase {
 public:
  pb::Output* mutable_msg() { return out_; }
  const pb::Output& msg() const { return *out_; }

 protected:
  pb::Output* out_;

  OutputBase(OutputBase&&) noexcept;
  OutputBase(pb::Output* out) : out_(out) {}

  void SetCompress(pb::Output::CompressType ct, unsigned level);
  void SetShardSpec(pb::ShardSpec::Type st, unsigned modn = 0);
};

template <typename T> class Output : public OutputBase {
  friend class detail::TableImpl<T>;  // To allow the instantiation of Output<T>;

  // TODO: to make it variant.
  std::function<std::string(const T&)> shard_op_;
  std::function<unsigned(const T&)> modn_op_;

 public:
  Output() : OutputBase(nullptr) {}
  Output(const Output&) = delete;
  Output(Output&&) noexcept;

  template <typename U> Output& WithCustomSharding(U&& func) {
    static_assert(base::is_invocable_r<std::string, U, const T&>::value, "");
    shard_op_ = std::forward<U>(func);
    SetShardSpec(pb::ShardSpec::USER_DEFINED);

    return *this;
  }

  template <typename U> Output& WithModNSharding(unsigned modn, U&& func) {
    static_assert(base::is_invocable_r<unsigned, U, const T&>::value, "");
    modn_op_ = std::forward<U>(func);
    SetShardSpec(pb::ShardSpec::MODN, modn);

    return *this;
  }

  Output& AndCompress(pb::Output::CompressType ct, unsigned level = 0);

  void operator=(const Output&) = delete;

  Output& operator=(Output&& o) {
    shard_op_ = std::move(o.shard_op_);
    modn_op_ = std::move(o.modn_op_);
    out_ = o.out_;
    return *this;
  }

  ShardId Shard(const T& t) {
    if (shard_op_)
      return shard_op_(t);
    else if (modn_op_)
      return modn_op_(t) % out_->shard_spec().modn();

    return ShardId{0};
  }

 private:
  Output(pb::Output* out) : OutputBase(out) {}
};

template <typename OutT>
Output<OutT>& Output<OutT>::AndCompress(pb::Output::CompressType ct, unsigned level) {
  SetCompress(ct, level);
  return *this;
}

inline OutputBase::OutputBase(OutputBase&&) noexcept = default;

template <typename T>
Output<T>::Output(Output&& o) noexcept
    : OutputBase(o.out_), shard_op_(std::move(o.shard_op_)), modn_op_(std::move(o.modn_op_)) {}

}  // namespace mr3

namespace std {

template <> struct hash<mr3::ShardId> {
  size_t operator()(const mr3::ShardId& sid) const { return hash<mr3::ShardId::Parent>{}(sid); }
};

}  // namespace std
