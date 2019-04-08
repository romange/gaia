// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <functional>

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

#include "strings/unique_strings.h"
#include "util/status.h"

namespace mr3 {
class Pipeline;
class TableBase;
class StringTable;

template <typename OutT> class TableImpl;
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
  friend class TableImpl<T>;

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

class TableBase {
 public:
  using RawRecord = std::string;
  typedef std::function<void(RawRecord&& record)> DoFn;

  TableBase(const std::string& nm, Pipeline* owner) : pipeline_(owner) { op_.set_op_name(nm); }

  TableBase(pb::Operator op, Pipeline* owner) : op_(std::move(op)), pipeline_(owner) {}

  virtual ~TableBase() {}

  virtual DoFn SetupDoFn(RawContext* context) = 0;

  // virtual util::Status InitializationStatus() const { return util::Status::OK; }

  const pb::Operator& op() const { return op_; }
  pb::Operator* mutable_op() { return &op_; }

  friend void intrusive_ptr_add_ref(TableBase* tbl) noexcept {
    tbl->use_count_.fetch_add(1, std::memory_order_relaxed);
  }

  friend void intrusive_ptr_release(TableBase* tbl) noexcept {
    if (1 == tbl->use_count_.fetch_sub(1, std::memory_order_release)) {
      // See connection_handler.h {intrusive_ptr_release} implementation
      // for memory barriers explanation.
      std::atomic_thread_fence(std::memory_order_acquire);
      delete tbl;
    }
  }

  Pipeline* pipeline() const { return pipeline_; }

 protected:
  pb::Operator op_;
  Pipeline* pipeline_;
  std::atomic<std::uint32_t> use_count_{0};

  void SetOutput(const std::string& name, pb::WireFormat::Type type);
};

// Currently the input type is hard-coded - string.
template <typename OutT> class TableImpl : public TableBase {
  template <typename T> friend class PTable;

 public:
  using OutputType = OutT;
  using ContextType = DoContext<OutT>;

  using PtrType = ::boost::intrusive_ptr<TableImpl<OutT>>;

  using TableBase::TableBase;

  Output<OutputType>& Write(const std::string& name, pb::WireFormat::Type type) {
    SetOutput(name, type);
    out_ = Output<OutputType>(op_.mutable_output());
    return out_;
  }

  template <typename U> typename TableImpl<U>::PtrType CloneAs(pb::Operator op) const {
    return new TableImpl<U>{std::move(op), pipeline_};
  }

 protected:
  DoFn SetupDoFn(RawContext* context) override;

 private:
  template <typename FromType, typename MapType> void MapWith();

  Output<OutT> out_;
  std::function<void(RawRecord&&, DoContext<OutputType>* context)> do_fn_;
};

template <typename OutT> class PTable {
 public:
  // TODO: to hide it from public interface.
  TableBase* impl() { return impl_.get(); }

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

  PTable(typename TableImpl<OutT>::PtrType ptr) : impl_(std::move(ptr)) {}

  typename TableImpl<OutT>::PtrType impl_;
};

class StringTable : public PTable<std::string> {
  friend class Pipeline;

 public:
  StringTable(PTable<std::string>&& p) : PTable(p.impl_) {}

  PTable<rapidjson::Document> AsJson() const;

  template<typename U> PTable<U> As() const;
 protected:
  StringTable(TableImpl<std::string>::PtrType ptr) : PTable(ptr) {}
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


  pb::Operator new_op = impl_->op();
  new_op.set_op_name(name);

  auto ptr = impl_->template CloneAs<NewOutType>(std::move(new_op));
  ptr->template MapWith<OutT, MapType>();

  return PTable<NewOutType>(ptr);
}

template <typename OutT> auto TableImpl<OutT>::SetupDoFn(RawContext* context) -> DoFn {
  if (do_fn_) {
    return [f = this->do_fn_, wrapper = ContextType(&out_, context)](RawRecord&& r) mutable {
      f(std::move(r), &wrapper);
    };
  } else {
    return [wrapper = ContextType(&out_, context)](RawRecord&& r) mutable {
      OutT val;
      if (wrapper.ParseRaw(std::move(r), &val))
        wrapper.Write(std::move(val));
    };
  }
}

template <> class RecordTraits<rapidjson::Document> {
  std::string tmp_;

 public:
  static std::string Serialize(rapidjson::Document&& doc);
  bool Parse(std::string&& tmp, rapidjson::Document* res);
};

template <typename OutT>
template <typename FromType, typename MapType>
void TableImpl<OutT>::MapWith() {
  struct Helper {
    MapType m;
    RecordTraits<FromType> rt;
  };

  do_fn_ = [h = Helper{}](RawRecord&& rr, DoContext<OutputType>* context) mutable {
    FromType tmp_rec;
    bool parse_res = context->ParseRaw(std::move(rr), &h.rt, &tmp_rec);
    if (parse_res) {
      h.m.Do(std::move(tmp_rec), context);
    }
  };
}

template<typename U> PTable<U> StringTable::As() const {
  pb::Operator new_op = impl_->op();
  typename TableImpl<U>::PtrType ptr(impl_->CloneAs<U>(std::move(new_op)));
  return PTable<U>{ptr};
}

}  // namespace mr3
