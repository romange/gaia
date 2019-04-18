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

#include "mr/do_context.h"
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

template <typename OutT> class PTable {
  using TableImpl = detail::TableImpl<OutT>;
  friend class Pipeline;

  // apparently template classes of different type can not access own private members.
  template <typename T> friend class PTable;
 public:
  PTable() {}
  PTable(PTable&&) = default;

  ~PTable() {}

  Output<OutT>& Write(const std::string& name, pb::WireFormat::Type type) {
    return impl_->Write(name, type);
  }

  template <typename MapType>
  PTable<typename detail::MapperTraits<MapType>::OutputType> Map(const std::string& name) const;

  template <typename Handler, typename ToType, typename U>
  detail::HandlerBinding<Handler, ToType> BindWith(EmitMemberFn<U, Handler, ToType> ptr) const {
    return impl_->BindWith(ptr);
  }

  template <typename U> PTable<U> As() const { return PTable<U>{impl_->template As<U>()}; }

  PTable<rapidjson::Document> AsJson() const { return As<rapidjson::Document>(); }

 private:
  explicit PTable(TableImpl* impl) : impl_(impl) {}

  // Dangerous convenience method. Consider remove it.
  explicit PTable(detail::TableBase* tb) : PTable{TableImpl::AsIdentity(tb)} {}

  std::unique_ptr<TableImpl> impl_;
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

  auto* res = detail::TableImpl<NewOutType>::template AsMapFrom<MapType>(name, impl_.get());
  return PTable<NewOutType>{res};
}

template <> class RecordTraits<rapidjson::Document> {
  std::string tmp_;

 public:
  static std::string Serialize(rapidjson::Document&& doc);
  bool Parse(std::string&& tmp, rapidjson::Document* res);
};

}  // namespace mr3
