// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include <functional>

#include "base/type_traits.h"
#include "mr/mr3.pb.h"
#include "util/status.h"

namespace mr3 {

class StreamBase;

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

template <typename T> class Output : public OutputBase {
  friend class StreamBase;

  std::function<std::string(const T&)> shard_op_;

 public:
  Output() : OutputBase(nullptr) {}
  Output(const Output&) = delete;
  Output(Output&&) noexcept = default;

  template <typename U> Output& WithSharding(U&& func) {
    static_assert(base::is_invocable_r<std::string, U, const T&>::value);
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

 private:
  Output(pb::Output* out) : OutputBase(out) {}
};  // namespace mr3

class StreamBase {
 protected:
  pb::Operator op_;

  template <typename T> Output<T> WriteInternal(const std::string& name) {
    op_.mutable_output()->set_name(name);
    return Output<T>(op_.mutable_output());
  }
};

template <typename T> class Stream : public StreamBase {
  Output<T> out_;

 public:
  Stream(const std::string& input_name) { op_.add_input_name(input_name); }

  Output<T>& Write(const std::string& name) {
    out_ = WriteInternal<T>(name);
    return out_;
  }
};

using StringStream = Stream<std::string>;

class Pipeline {
 public:
  StringStream& ReadText(const std::string& name, const std::string& glob);

  util::Status Run();

  const InputBase& input(const std::string& name) const ;

 private:
  std::vector<std::unique_ptr<InputBase>> inputs_;
  std::vector<std::unique_ptr<StringStream>> streams_;
};

template <typename T>
Output<T>& Output<T>::AndCompress(pb::Output::CompressType ct, unsigned level) {
  SetCompress(ct, level);
  return *this;
}

}  // namespace mr3
