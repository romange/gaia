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
#include "mr/output.h"
#include "util/fibers/simple_channel.h"

// #include "strings/unique_strings.h"
// #include "util/status.h"

namespace mr3 {

class Pipeline;
class StringTable;

template <typename T> class DoContext;
template <typename T> class PTable;

using RawRecord = std::string;

namespace detail {
template <typename T> struct DoCtxResolver : public std::false_type {};
template <typename T> struct DoCtxResolver<DoContext<T>*> : public std::true_type {
  using OutType = T;
};

template <typename Func> struct EmitFuncTraits {
  using do_traits_t = base::function_traits<Func>;

  static_assert(do_traits_t::arity == 2, "MapperType::Do must accept 2 arguments");

  using first_arg_t = typename do_traits_t::template arg<0>;
  using second_arg_t = typename do_traits_t::template arg<1>;

  static_assert(DoCtxResolver<second_arg_t>::value,
                "MapperType::Do's second argument should be "
                "DoContext<T>* for some type T");
  using OutputType = typename DoCtxResolver<second_arg_t>::OutType;
};

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
// It's packaged together with the DoFn function.
template <typename T> class DoContext {
  friend class detail::TableImpl<T>;

  DoContext(Output<T>* out, RawContext* context) : out_(out), context_(context) {}

 public:
  void Write(T&& t) {
    ShardId shard_id = out_->Shard(t);
    std::string dest = rt_.Serialize(std::move(t));
    context_->WriteInternal(shard_id, std::move(dest));
  }

  RawContext* raw_context() { return context_; }

 private:
  bool ParseRaw(RawRecord&& rr, T* res) { return context_->ParseInto(std::move(rr), &rt_, res); }

  Output<T>* out_;
  RawContext* context_;
  RecordTraits<T> rt_;
};

template <typename Handler, typename ToType> class HandlerBinding;

template <typename OutT> class PTable {
 public:
  // TODO: to free input parameter as well.
  template <typename Class, typename O> using DoFn = void (Class::*)(OutT&&, DoContext<O>*);

  Output<OutT>& Write(const std::string& name, pb::WireFormat::Type type) {
    return impl_->Write(name, type);
  }

  template <typename MapType>
  PTable<typename detail::MapperTraits<MapType>::OutputType> Map(const std::string& name) const;

  template <typename Handler, typename ToType>
  HandlerBinding<Handler, ToType> BindWith(DoFn<Handler, ToType> ptr) const {
    return HandlerBinding<Handler, ToType>{impl_.get(), ptr};
  }

 protected:
  friend class Pipeline;
  friend class StringTable;

  // apparently PTable of different type can not access this members.
  template <typename T> friend class PTable;
  using TableImpl = detail::TableImpl<OutT>;

  PTable(typename TableImpl::PtrType ptr) : impl_(std::move(ptr)) {}

  typename TableImpl::PtrType impl_;
};

class StringTable : public PTable<std::string> {
  friend class Pipeline;

 public:
  StringTable(PTable<std::string>&& p) : PTable(p.impl_) {}

  template <typename U> PTable<U> As() const { return PTable<U>{impl_->CloneAs<U>()}; }

  PTable<rapidjson::Document> AsJson() const { return As<rapidjson::Document>(); }

 protected:
  StringTable(TableImpl::PtrType ptr) : PTable(ptr) {}
};

template <typename OutT>
template <typename MapType>
PTable<typename detail::MapperTraits<MapType>::OutputType> PTable<OutT>::Map(
    const std::string& name) const {
  using mapper_traits_t = detail::MapperTraits<MapType>;
  using NewOutType = typename mapper_traits_t::OutputType;

  static_assert(std::is_constructible<typename mapper_traits_t::first_arg_t, OutT&&>::value,
                "MapperType::Do() first argument "
                "should be constructed from PTable element type");

  bool read_from_input = impl_->is_identity();
  pb::Operator new_op = impl_->CreateLink(!read_from_input);
  new_op.set_op_name(name);
  new_op.set_type(pb::Operator::MAP);

  auto ptr = impl_->template CloneAs<NewOutType>(std::move(new_op));
  ptr->template MapWith<OutT, MapType>();

  return PTable<NewOutType>(ptr);
}

template <> class RecordTraits<rapidjson::Document> {
  std::string tmp_;

 public:
  static std::string Serialize(rapidjson::Document&& doc);
  bool Parse(std::string&& tmp, rapidjson::Document* res);
};

template <typename Handler, typename ToType> class HandlerBinding {
 public:
  using EmitFunc = std::function<void(RawRecord&&, DoContext<ToType>* context)>;
  using SetupEmitFunc = std::function<EmitFunc(Handler* handler)>;

  template <typename FromType>
  HandlerBinding(const detail::TableImpl<FromType>* from,
                 typename PTable<FromType>::template DoFn<Handler, ToType> ptr) {
    tbase_from = from;
    setup_func = [ptr](Handler* handler) {
      auto f = [ptr, handler, rt = RecordTraits<FromType>{}](RawRecord&& rr,
                                                             DoContext<ToType>* context) mutable {
        FromType tmp_rec;
        bool parse_res = context->raw_context()->ParseInto(std::move(rr), &rt, &tmp_rec);
        if (parse_res) {
          ((*handler).*ptr)(std::move(tmp_rec), context);
        }
      };
      return f;
    };
  }

  const detail::TableBase* tbase_from;
  SetupEmitFunc setup_func;
};

}  // namespace mr3
