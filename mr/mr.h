// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <boost/smart_ptr/intrusive_ptr.hpp>

// I do not enable SSE42 for rapidjson because it may go out of boundaries, they assume
// all the inputs are aligned at the end.
//
// #define RAPIDJSON_SSE42

#include <rapidjson/document.h>

#include "base/type_traits.h"
#include "file/file.h"
#include "mr/impl/table_impl.h"
#include "mr/mr_types.h"
#include "mr/output.h"
#include "util/fibers/simple_channel.h"

namespace mr3 {

class Pipeline;

namespace detail {

template <typename MapperType>
struct MapperTraits : public EmitFuncTraits<decltype(&MapperType::Do)> {};

}  // namespace detail

using RecordQueue = util::fibers_ext::SimpleChannel<std::string>;

// Planning interfaces.
class InputBase {
 public:
  InputBase(const InputBase&) = delete;

  InputBase(const std::string& name, pb::WireFormat::Type type,
            const pb::Output* linked_outp = nullptr)
      : linked_outp_(linked_outp) {
    input_.set_name(name);
    input_.mutable_format()->set_type(type);
  }

  void operator=(const InputBase&) = delete;

  pb::Input* mutable_msg() { return &input_; }
  const pb::Input& msg() const { return input_; }

  const pb::Output* linked_outp() const { return linked_outp_; }

 protected:
  const pb::Output* linked_outp_;
  pb::Input input_;
};

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

  virtual void Flush() {}

  size_t parse_errors = 0;

  // Returns true if succeeded.
  template <typename U> bool ParseInto(RawRecord&& rr, RecordTraits<U>* rt, U* res) {
    bool parse_res = rt->Parse(std::move(rr), res);
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

  template <typename U> friend class detail::IdentityHandlerWrapper;

  DoContext(const Output<T>& out, RawContext* context) : out_(out), context_(context) {}

 public:
  void Write(T&& t) {
    ShardId shard_id = out_.Shard(t);
    std::string dest = rt_.Serialize(std::move(t));
    context_->WriteInternal(shard_id, std::move(dest));
  }

  RawContext* raw_context() { return context_; }

 private:
  bool ParseRaw(RawRecord&& rr, T* res) { return context_->ParseInto(std::move(rr), &rt_, res); }

  Output<T> out_;
  RawContext* context_;
  RecordTraits<T> rt_;
};

template <typename OutT> class PTable {
  struct Impl {
    Output<OutT> output;
    std::unique_ptr<detail::TableBase> table;

    Impl(detail::TableBase* tb) : table(tb) {}
  };

 public:
  PTable() {}
  PTable(PTable&&) = default;

  ~PTable() {}

  Output<OutT>& Write(const std::string& name, pb::WireFormat::Type type) {
    impl_->table->SetOutput(name, type);
    impl_->output = Output<OutT>{impl_->table->mutable_op()->mutable_output()};
    return impl_->output;
  }

  template <typename MapType>
  PTable<typename detail::MapperTraits<MapType>::OutputType> Map(const std::string& name) const;

  template <typename Handler, typename ToType, typename U>
  detail::HandlerBinding<Handler, ToType> BindWith(EmitMemberFn<U, Handler, ToType> ptr) const {
    return detail::HandlerBinding<Handler, ToType>::template Create<OutT>(impl_->table.get(), ptr);
  }

  template <typename U> PTable<U> As() const {
    impl_->table->CheckFailIdentity();
    return PTable<U>::AsIdentity(impl_->table->Clone());
  }

  PTable<rapidjson::Document> AsJson() const { return As<rapidjson::Document>(); }

 protected:
  friend class Pipeline;

  // apparently PTable of different type can not access this members.
  template <typename T> friend class PTable;
  using TableImpl = detail::TableBase;

  PTable(TableImpl* ptr) : impl_(new Impl{ptr}) {}

  static PTable<OutT> AsIdentity(TableImpl* ptr) {
    PTable<OutT> res(ptr);
    res.impl_->table->SetIdentity(&res.impl_->output);
    return res;
  }

  // Map c'tor
  template <typename FromType, typename MapType> static PTable<OutT> AsMap(TableImpl* ptr) {
    PTable<OutT> res(ptr);
    ptr->SetHandlerFactory([o = &res.impl_->output](RawContext* raw_ctxt) {
      auto* ptr = new detail::HandlerWrapper<MapType, OutT>(*o, raw_ctxt);
      ptr->template Add<FromType>(&MapType::Do);
      return ptr;
    });

    return res;
  }

  std::unique_ptr<Impl> impl_;

 private:
  template <typename JoinerType>
  static PTable<OutT> AsJoin(TableImpl* ptr,
                             std::vector<RawSinkMethodFactory<JoinerType, OutT>> factories);
};

using StringTable = PTable<std::string>;

template <typename OutT>
template <typename MapType>
PTable<typename detail::MapperTraits<MapType>::OutputType> PTable<OutT>::Map(
    const std::string& name) const {
  using mapper_traits_t = detail::MapperTraits<MapType>;
  using NewOutType = typename mapper_traits_t::OutputType;

  static_assert(std::is_constructible<typename mapper_traits_t::first_arg_t, OutT&&>::value,
                "MapperType::Do() first argument "
                "should be constructed from PTable element type");

  pb::Operator new_op = impl_->table->GetDependeeOp();
  new_op.set_op_name(name);
  new_op.set_type(pb::Operator::MAP);

  detail::TableBase* ptr = new detail::TableBase(std::move(new_op), impl_->table->pipeline());

  return PTable<NewOutType>::template AsMap<OutT, MapType>(ptr);
}

template <typename OutT>
template <typename JoinerType>
PTable<OutT> PTable<OutT>::AsJoin(TableImpl* ptr,
                                  std::vector<RawSinkMethodFactory<JoinerType, OutT>> factories) {
  PTable<OutT> res(ptr);
  ptr->SetHandlerFactory(
      [o = &res.impl_->output, factories = std::move(factories)](RawContext* raw_ctxt) {
        auto* ptr = new detail::HandlerWrapper<JoinerType, OutT>(*o, raw_ctxt);
        for (const auto& m : factories) {
          ptr->AddFromFactory(m);
        }
        return ptr;
      });
  return res;
}

template <> class RecordTraits<rapidjson::Document> {
  std::string tmp_;

 public:
  static std::string Serialize(rapidjson::Document&& doc);
  bool Parse(std::string&& tmp, rapidjson::Document* res);
};

}  // namespace mr3
