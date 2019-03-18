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

class TableBase;
template <typename OutT> class Stream;
template <typename T> class DoContext;

// Planning interfaces.
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

// User facing interfaces
template <typename Record> struct RecordTraits {
  static std::string Serialize(Record&& r) { return std::string(std::move(r)); }
};

class RawContext {
  template <typename T> friend class DoContext;
 public:
  virtual ~RawContext();

 protected:
  virtual void WriteInternal(const ShardId& shard_id, std::string&& record) = 0;
};


// Right now this class is not thread-local and is access by all threads concurrently
// i.e. all its method must be thread-safe.
template <typename T> class DoContext {
  Output<T>* out_;
  RawContext* context_;
  RecordTraits<T> rt_;

  friend class Stream<T>;

  DoContext(Output<T>* out, RawContext* context) : out_(out), context_(context) {}

 public:
  void Write(T&& t) {
    ShardId shard_id = out_->Shard(t);
    std::string dest = rt_.Serialize(std::move(t));
    context_->WriteInternal(shard_id, std::move(dest));
  }
};

class TableBase {
 public:
  using RawRecord = std::string;
  typedef std::function<void(RawRecord&& record)> DoFn;

  virtual ~TableBase() {}
  virtual DoFn SetupDoFn(RawContext* context) = 0;

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
template <typename OutT> class Stream : public TableBase {
  Output<OutT> out_;

 public:
  using InputType = RawRecord;
  using OutputType = OutT;
  using ContextType = DoContext<OutT>;

  Stream(const std::string& input_name) { op_.add_input_name(input_name); }

  Output<OutputType>& Write(const std::string& name, pb::WireFormat::Type type) {
    SetOutput(name, type);
    out_ = Output<OutputType>(op_.mutable_output());
    return out_;
  }

  Output<OutputType>& output() { return out_; }

  template <typename Func> Stream& Apply(Func&& f) {
    static_assert(base::is_invocable_r<void, Func, InputType&&, DoContext<OutputType>*>::value,
                  "");
    do_fn_ = std::forward<Func>(f);
    return *this;
  }

  util::Status InitializationStatus() const override;

 protected:
  DoFn SetupDoFn(RawContext* context) override;

 private:
  std::function<void(RawRecord&&, DoContext<OutputType>* context)> do_fn_;
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

template <typename OutT> auto Stream<OutT>::SetupDoFn(RawContext* context) -> DoFn {
  if (do_fn_) {
    return [f = this->do_fn_, wrapper = DoContext<OutT>(&out_, context)]
    (RawRecord&& r) mutable {
      f(std::move(r), &wrapper);
    };
  } else {
    return [wrapper = DoContext<OutT>(&out_, context)]
    (RawRecord&& r) mutable {
      wrapper.Write(std::move(r));
    };
  }
}

}  // namespace mr3
