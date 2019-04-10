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
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include "base/type_traits.h"
#include "file/file.h"
#include "mr/output.h"
#include "mr/impl/table_impl.h"

#include "strings/unique_strings.h"
#include "util/status.h"

namespace mr3 {

class Pipeline;
class StringTable;

template <typename T> class DoContext;
template <typename T> class PTable;

namespace detail {
template <typename T> struct IsDoCtxHelper : public std::false_type {};
template <typename T> struct IsDoCtxHelper<DoContext<T>*> : public std::true_type {
  using OutType = T;
};

template <typename MapperType> struct MapperTraits {
  using do_raits_t = base::function_traits<decltype(&MapperType::Do)>;

  // arg0 is 'this' of MapperType.
  static_assert(do_raits_t::arity == 3, "MapperType::Do must accept 2 arguments");

  using first_arg_t = typename do_raits_t::template argument_type<1>;
  using second_arg_t = typename do_raits_t::template argument_type<2>;

  static_assert(IsDoCtxHelper<second_arg_t>::value,
                "MapperType::Do's second argument should be "
                "DoContext<T>* for some type T");
  using OutputType = typename IsDoCtxHelper<second_arg_t>::OutType;
};

}  // namespace detail

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

 protected:
  virtual void WriteInternal(const ShardId& shard_id, std::string&& record) = 0;
};

// This class is created per MapFiber in SetupDoFn and it wraps RawContext.
// It's packaged together with the DoFn function.
template <typename T> class DoContext {
  friend class detail::TableImpl<T>;

  DoContext(Output<T>* out, RawContext* context) : out_(out), context_(context) {}

 public:
  using RawRecord = std::string;

  void Write(T&& t) {
    ShardId shard_id = out_->Shard(t);
    std::string dest = rt_.Serialize(std::move(t));
    context_->WriteInternal(shard_id, std::move(dest));
  }

 private:
  bool ParseRaw(RawRecord&& rr, T* res) { return ParseRaw(std::move(rr), &rt_, res); }

  template <typename U> bool ParseRaw(RawRecord&& rr, RecordTraits<U>* rt, U* res) {
    bool parse_res = rt->Parse(std::move(rr), res);
    if (!parse_res)
      ++context_->parse_errors;

    return parse_res;
  }

  Output<T>* out_;
  RawContext* context_;
  RecordTraits<T> rt_;
};


template <typename OutT> class PTable {
 public:
  Output<OutT>& Write(const std::string& name, pb::WireFormat::Type type) {
    return impl_->Write(name, type);
  }

  template <typename MapType>
  PTable<typename detail::MapperTraits<MapType>::OutputType> Map(
      const std::string& name) const;

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

  template<typename U> PTable<U> As() const {
    return PTable<U>{impl_->CloneAs<U>()};
  }

  PTable<rapidjson::Document> AsJson() const {
    return As<rapidjson::Document>();
  }


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

}  // namespace mr3
