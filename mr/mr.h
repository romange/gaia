// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <functional>

#include "absl/types/variant.h"
#include "base/type_traits.h"
#include "file/file.h"
#include "mr/mr3.pb.h"

#include "strings/unique_strings.h"
#include "util/status.h"

namespace mr3 {

class StreamBase;
template <typename OutT> class Stream;


template<typename Record> struct RecordTraits {
  static std::string Serialize(Record&& r) {
    return std::string(std::move(r));
  }
};

class InputBase {
 public:
  InputBase(const InputBase&) = delete;

  InputBase(const std::string& name, pb::WireFormat::Type type) {
    input_.set_name(name);
    input_.mutable_format()->set_type(type);
  }

  pb::Input* mutable_msg() { return &input_; }
  const pb::Input& msg() const { return input_; }

  void operator=(const InputBase&) = delete;

 protected:
  pb::Input input_;
};

using ShardId = absl::variant<uint32_t, std::string>;

class OutputBase {
 public:
  pb::Output* mutable_msg() { return out_; }
  const pb::Output& msg() const { return *out_; }

  virtual ~OutputBase() {}

 protected:
  pb::Output* out_;

  OutputBase(pb::Output* out) : out_(out) {}

  void SetCompress(pb::Output::CompressType ct, unsigned level) {
    auto* co = out_->mutable_compress();
    co->set_type(ct);
    if (level) {
      co->set_level(level);
    }
  }
};

template <typename T> class Output : public OutputBase {
  friend class Stream<T>;

  std::function<std::string(const T&)> shard_op_;

 public:
  Output() : OutputBase(nullptr) {}
  Output(const Output&) = delete;
  Output(Output&&) noexcept = default;

  template <typename U> Output& WithSharding(U&& func) {
    static_assert(base::is_invocable_r<std::string, U, const T&>::value, "");
    shard_op_ = std::forward<U>(func);
    mutable_msg()->set_shard_type(pb::Output::USER_DEFINED);

    return *this;
  }

  Output& AndCompress(pb::Output::CompressType ct, unsigned level = 0);

  void operator=(const Output&) = delete;

  Output& operator=(Output&& o) {
    shard_op_ = std::move(o.shard_op_);
    out_ = o.out_;
    return *this;
  }

  ShardId Shard(const T& t) {
    if (shard_op_)
      return shard_op_(t);

    // TODO: Hasher<T>(t) & msg().modn();
    return ShardId{0};
  }

 private:
  Output(pb::Output* out) : OutputBase(out) {}
};

class DoContext {
 public:
  virtual ~DoContext();

  virtual void Write(const ShardId& shard_id, std::string&& record) = 0;
};

class StreamBase {
 public:
  virtual ~StreamBase() {}
  virtual void Do(std::string&& record, DoContext* context) = 0;

  virtual util::Status InitializationStatus() const { return util::Status::OK; }
 protected:
  pb::Operator op_;

  void SetOutput(const std::string& name, pb::WireFormat::Type type) {
    auto* out = op_.mutable_output();
    out->set_name(name);
    out->mutable_format()->set_type(type);
  }
};

// Currently the input type is hard-coded - string.
template <typename OutT> class Stream : public StreamBase {
  Output<OutT> out_;

 public:
  using InputType = std::string;
  using OutputType = OutT;

  Stream(const std::string& input_name) { op_.add_input_name(input_name); }

  Output<OutputType>& Write(const std::string& name, pb::WireFormat::Type type) {
    SetOutput(name, type);
    out_ = Output<OutputType>(op_.mutable_output());
    return out_;
  }

  Output<OutputType>& output() { return out_; }

  template<typename Func> Stream& Apply(Func&& f) {
    static_assert(base::is_invocable_r<OutputType, Func, InputType&&>::value, "");
    do_fn_ = std::forward<Func>(f);
  }

  util::Status InitializationStatus() const override;

 protected:
  void Do(std::string&& record, DoContext* context);

 private:
  std::function<OutputType(InputType&&)> do_fn_;
};

using StringStream = Stream<std::string>;

class Pipeline {
 public:
  StringStream& ReadText(const std::string& name, const std::string& glob);
  StringStream& ReadText(const std::string& name, const std::vector<std::string>& globs);

  const InputBase& input(const std::string& name) const;

 private:
  std::vector<std::unique_ptr<InputBase>> inputs_;
  std::vector<std::unique_ptr<StringStream>> streams_;
};

template <typename OutT>
Output<OutT>& Output<OutT>::AndCompress(pb::Output::CompressType ct, unsigned level) {
  SetCompress(ct, level);
  return *this;
}

template <typename OutT> util::Status Stream<OutT>::InitializationStatus() const {
  if (!std::is_same<OutputType, InputType>::value) {
    if (!do_fn_)
      return util::Status("Apply function is not set");
  }
  return util::Status::OK;
}


template <typename OutT> void Stream<OutT>::Do(std::string&& record, DoContext* context) {
  // TODO: to parse from record to InputType.
  ShardId shard_id;

  OutputType val;
  if (do_fn_) {
    val = do_fn_(std::move(record));
  } else {
    val = std::move(record);
  }

  shard_id = out_.Shard(val);
  RecordTraits<OutputType> rt;
  std::string dest = rt.Serialize(std::move(val));
  context->Write(shard_id, std::move(dest));
}

}  // namespace mr3
