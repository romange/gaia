// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <functional>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>
#include "absl/types/variant.h"

#include "base/type_traits.h"
#include "file/file.h"
#include "mr/mr3.pb.h"

#include "strings/unique_strings.h"
#include "util/status.h"

namespace mr3 {

class TableBase;
class StringTable;
template <typename OutT> class TableImpl;
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

  OutputBase(OutputBase&&) noexcept;
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
  friend class TableImpl<T>; // To allow the instantiation of Output<T>;

  std::function<std::string(const T&)> shard_op_;

 public:
  Output() : OutputBase(nullptr) {}
  Output(const Output&) = delete;
  Output(Output&&) noexcept;

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

  static Record Parse(std::string&& tmp) {
    return std::move(tmp);
  }
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

  friend class TableImpl<T>;

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

  TableBase(const std::string& nm) {
    op_.set_op_name(nm);
  }

  virtual ~TableBase() {}

  virtual DoFn SetupDoFn(RawContext* context) = 0;

  virtual util::Status InitializationStatus() const { return util::Status::OK; }

  const pb::Operator& op() const { return op_;}

 protected:
  pb::Operator op_;

  void SetOutput(const std::string& name, pb::WireFormat::Type type) {
    auto* out = op_.mutable_output();
    out->set_name(name);
    out->mutable_format()->set_type(type);
  }
};

// Currently the input type is hard-coded - string.
template <typename OutT> class TableImpl : public TableBase {
  Output<OutT> out_;

 public:
  using OutputType = OutT;
  using ContextType = DoContext<OutT>;

  // Could be intrusive_ptr as well.
  using PtrType = std::shared_ptr<TableImpl<OutT>>;

  TableImpl(const std::string& name) : TableBase(name) {}

  Output<OutputType>& Write(const std::string& name, pb::WireFormat::Type type) {
    SetOutput(name, type);
    out_ = Output<OutputType>(op_.mutable_output());
    return out_;
  }

  util::Status InitializationStatus() const override;

 protected:
  DoFn SetupDoFn(RawContext* context) override;

 private:
  std::function<void(RawRecord&&, DoContext<OutputType>* context)> do_fn_;
};

template <typename OutT> class PTable {
public:
  // TODO: to hide it from public interface.
  TableBase* impl() { return impl_.get(); }

  Output<OutT>& Write(const std::string& name, pb::WireFormat::Type type) {
    return impl_->Write(name, type);
  }

protected:
  friend class Pipeline;
  friend class StringTable;

  PTable(typename TableImpl<OutT>::PtrType ptr) : impl_(std::move(ptr)) {}

  typename TableImpl<OutT>::PtrType impl_;
};

class StringTable : public PTable<std::string> {
  friend class Pipeline;

 public:
  PTable<rapidjson::Document> AsJson() const;

 protected:
  StringTable(TableImpl<std::string>::PtrType ptr) : PTable(ptr) {}
};

class Pipeline {
 public:
  StringTable ReadText(const std::string& name, const std::vector<std::string>& globs);

  StringTable ReadText(const std::string& name, const std::string& glob) {
    return ReadText(name, std::vector<std::string>{glob});
  }

  const InputBase& input(const std::string& name) const;

 private:
  std::vector<std::unique_ptr<InputBase>> inputs_;
  std::vector<std::shared_ptr<TableBase>> tables_;
};

template <typename OutT>
Output<OutT>& Output<OutT>::AndCompress(pb::Output::CompressType ct, unsigned level) {
  SetCompress(ct, level);
  return *this;
}

template <typename OutT> util::Status TableImpl<OutT>::InitializationStatus() const {
  if (!std::is_same<OutputType, RawRecord>::value) {
    if (!do_fn_)
      return util::Status("Apply function is not set");
  }
  return util::Status::OK;
}

template <typename OutT> auto TableImpl<OutT>::SetupDoFn(RawContext* context) -> DoFn {
  if (do_fn_) {
    return [f = this->do_fn_, wrapper = DoContext<OutT>(&out_, context)]
    (RawRecord&& r) mutable {
      f(std::move(r), &wrapper);
    };
  } else {
    return [wrapper = DoContext<OutT>(&out_, context)]
    (RawRecord&& r) mutable {
      wrapper.Write(RecordTraits<OutT>::Parse(std::move(r)));
    };
  }
}

inline OutputBase::OutputBase(OutputBase&&) noexcept = default;
template <typename T> Output<T>::Output(Output&&) noexcept = default;

template <> struct RecordTraits<rapidjson::Document> {

  static std::string Serialize(rapidjson::Document&& doc) {
    rapidjson::StringBuffer s;
    rapidjson::Writer<rapidjson::StringBuffer> writer(s);
    doc.Accept(writer);

    std::string res = s.GetString();
    res.push_back('\n');
    return res;
  }

  static rapidjson::Document Parse(std::string&& tmp) {
    rapidjson::Document doc;
    constexpr unsigned kFlags = rapidjson::kParseTrailingCommasFlag | rapidjson::kParseCommentsFlag;
    doc.Parse<kFlags>(tmp.c_str(), tmp.size());
    return doc;
  }
};


}  // namespace mr3
