// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <functional>

#include "base/type_traits.h"
#include "file/file.h"
#include "mr/mr3.pb.h"

#include "strings/unique_strings.h"
#include "util/status.h"

namespace mr3 {

class StreamBase;
class ExecutionOutputContext;

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

class OutputBase {
  friend class ExecutionOutputContext;

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

  /*struct RecordResult {
    std::string out;
    std::string file_key;
  };

  virtual void WriteInternal(std::string&& record, RecordResult* res) = 0;*/
};

class ExecutionOutputContext {
 public:
  explicit ExecutionOutputContext(const std::string& root_dir, OutputBase* ob);

  void WriteRecord(std::string&& record);

  OutputBase* output() { return ob_; }

 private:
  const std::string root_dir_;
  OutputBase* ob_;

  StringPieceDenseMap<file::WriteFile*> files_;
};

template <typename T> class Output : public OutputBase {
  friend class StreamBase;

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

  std::string Shard(const T& t) { return shard_op_ ? shard_op_(t) : std::string{}; }

 private:
  Output(pb::Output* out) : OutputBase(out) {}

  // void WriteInternal(std::string&& record, RecordResult* res) final;
};

struct ShardId {
  std::string key;
};

class DoContext {
 public:
  virtual ~DoContext() {}

  virtual void Write(const ShardId& shard_id, const std::string& record) = 0;
};

class StreamBase {
 public:
  virtual ~StreamBase() {}
  virtual void Do(std::string&& record, DoContext* context) = 0;

 protected:
  pb::Operator op_;

  template <typename T>
  Output<T> WriteInternal(const std::string& name, pb::WireFormat::Type type) {
    auto* out = op_.mutable_output();
    out->set_name(name);
    out->mutable_format()->set_type(type);
    return Output<T>(out);
  }
};

template <typename T> class Stream : public StreamBase {
  Output<T> out_;

 public:
  Stream(const std::string& input_name) { op_.add_input_name(input_name); }

  Output<T>& Write(const std::string& name, pb::WireFormat::Type type) {
    out_ = WriteInternal<T>(name, type);
    return out_;
  }

  Output<T>& output() { return out_; }

 protected:
  void Do(std::string&& record, DoContext* context);
};

using StringStream = Stream<std::string>;

class Pipeline {
 public:
  StringStream& ReadText(const std::string& name, const std::string& glob);

  util::Status Run();

  const InputBase& input(const std::string& name) const;

 private:
  std::vector<std::unique_ptr<InputBase>> inputs_;
  std::vector<std::unique_ptr<StringStream>> streams_;
};

template <typename T>
Output<T>& Output<T>::AndCompress(pb::Output::CompressType ct, unsigned level) {
  SetCompress(ct, level);
  return *this;
}

/*
template <typename T> void Output<T>::WriteInternal(std::string&& record, RecordResult* res) {
  res->out = std::move(record);
  if (shard_op_)
    res->file_key = shard_op_(record);
}
*/

template <typename T> void Stream<T>::Do(std::string&& record, DoContext* context) {
  ShardId shard_id;
  shard_id.key = out_.Shard(record);
  context->Write(shard_id, record);
}

}  // namespace mr3
