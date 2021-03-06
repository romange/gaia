// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

// I do not enable SSE42 for rapidjson because it may go out of boundaries, they assume
// all the inputs are aligned at the end.
//
// #define RAPIDJSON_SSE42

#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>

#include "base/type_traits.h"
#include "mr/do_context.h"
#include "mr/impl/table_impl.h"
#include "mr/mr_types.h"
#include "mr/output.h"

namespace mr3 {

class Pipeline;

namespace detail {

template <typename MapperType, typename = void> struct MapperTraits {
  static_assert(sizeof(MapperType) == 0,
                "Must have member function Do(InputType inp, DoContext<OutputType>* context)");
};

template <typename MapperType>
struct MapperTraits<MapperType, ::base::void_t<decltype(&MapperType::Do)>>
    : public EmitFuncTraits<decltype(&MapperType::Do)> {};

}  // namespace detail

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

template <typename OutT> class PTable {
  friend class Pipeline;

  // apparently template classes of different type can not access own private members.
  template <typename T> friend class PTable;

 public:
  PTable() {}
  ~PTable() {}

  Output<OutT>& Write(const std::string& name, pb::WireFormat::Type type) {
    return impl_->Write(name, type);
  }

  template <typename MapType, typename... Args>
  PTable<typename detail::MapperTraits<MapType>::OutputType> Map(const std::string& name,
                                                                 Args&&... args) const;

  template <typename Handler, typename ToType, typename U>
  detail::HandlerBinding<Handler, ToType> BindWith(EmitMemberFn<U, Handler, ToType> ptr) const {
    return impl_->BindWith(ptr);
  }

  template <typename U> PTable<U> As() const { return PTable<U>{impl_->template Rebind<U>()}; }

  PTable<rapidjson::Document> AsJson() const { return As<rapidjson::Document>(); }

 protected:
  using TableImpl = detail::TableImplT<OutT>;

  explicit PTable(std::shared_ptr<TableImpl> impl) : impl_(std::move(impl)) {}

  std::shared_ptr<TableImpl> impl_;
};

using StringTable = PTable<std::string>;

template <typename OutT>
template <typename MapType, typename... Args>
PTable<typename detail::MapperTraits<MapType>::OutputType> PTable<OutT>::Map(
    const std::string& name, Args&&... args) const {
  using mapper_traits_t = detail::MapperTraits<MapType>;
  using NewOutType = typename mapper_traits_t::OutputType;

  static_assert(std::is_constructible<typename mapper_traits_t::first_arg_t, OutT&&>::value,
                "MapperType::Do() first argument "
                "should be constructed from PTable element type");

  auto res = detail::TableImplT<NewOutType>::template AsMapFrom<MapType>(
      name, impl_.get(), std::forward<Args>(args)...);
  return PTable<NewOutType>{std::move(res)};
}

template <> class RecordTraits<rapidjson::Document> {
  std::string tmp_;
  rapidjson::StringBuffer sb_;  // Used by serialize.

 public:
  RecordTraits(const RecordTraits& r) {}  // we do not copy temporary fields.
  RecordTraits() {}

  std::string Serialize(bool is_binary, const rapidjson::Document& doc);
  bool Parse(bool is_binary, std::string&& tmp, rapidjson::Document* res);
};

}  // namespace mr3
