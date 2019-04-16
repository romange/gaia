// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <boost/intrusive_ptr.hpp>
#include <functional>

#include "mr/mr_types.h"
#include "mr/output.h"

namespace mr3 {
class Pipeline;
class RawContext;

template <typename Record> struct RecordTraits;

namespace detail {

template <typename Handler, typename ToType> class HandlerBinding;

template <typename T> struct DoCtxResolver : public std::false_type {};

template <typename T> struct DoCtxResolver<DoContext<T>*> : public std::true_type {
  using OutType = T;
};


template <typename Func> struct EmitFuncTraits {
  using emit_traits_t = base::function_traits<Func>;

  static_assert(emit_traits_t::arity == 2, "MapperType::Do must accept 2 arguments");

  using first_arg_t = typename emit_traits_t::template arg<0>;
  using second_arg_t = typename emit_traits_t::template arg<1>;

  static_assert(DoCtxResolver<second_arg_t>::value,
                "MapperType::Do's second argument should be "
                "DoContext<T>* for some type T");
  using OutputType = typename DoCtxResolver<second_arg_t>::OutType;
};

class HandlerWrapperBase {
 public:
  virtual ~HandlerWrapperBase() {}

  void Do(size_t index, RawRecord&& s) { raw_fn_[index](std::move(s)); }
  RawSinkCb Get(size_t index) const { return raw_fn_[index]; }

  size_t Size() const { return raw_fn_.size(); }

 protected:
  template <typename F> void AddFn(F&& f) { raw_fn_.emplace_back(std::forward<F>(f)); }

 private:
  std::vector<RawSinkCb> raw_fn_;
};

template <typename Handler, typename FromType, typename ToType, typename U>
void ParseAndDo(Handler* h, RecordTraits<FromType>* rt, DoContext<ToType>* context,
                EmitMemberFn<U, Handler, ToType> ptr, RawRecord&& rr) {
  FromType tmp_rec;
  bool parse_ok = context->raw_context()->ParseInto(std::move(rr), rt, &tmp_rec);
  if (parse_ok) {
    (h->*ptr)(std::move(tmp_rec), context);
  }
}

template <typename Handler, typename ToType> class HandlerWrapper : public HandlerWrapperBase {
  Handler h_;
  DoContext<ToType> do_ctx_;

 public:
  HandlerWrapper(Output<ToType>* out, RawContext* raw_context) : do_ctx_(out, raw_context) {}

  template <typename FromType, typename F> void Add(void (Handler::*ptr)(F, DoContext<ToType>*)) {
    AddFn([this, ptr, rt = RecordTraits<FromType>{}](RawRecord&& rr) mutable {
      ParseAndDo(&h_, &rt, &do_ctx_, ptr, std::move(rr));
    });
  }
};

template <typename T> class IdentityHandlerWrapper : public HandlerWrapperBase {
  DoContext<T> do_ctx_;

 public:
  IdentityHandlerWrapper(Output<T>* out, RawContext* raw_context) : do_ctx_(out, raw_context) {
    AddFn([this](RawRecord&& rr) {
      T val;
      if (do_ctx_.ParseRaw(std::move(rr), &val)) {
        do_ctx_.Write(std::move(val));
      }
    });
  }
};

class TableBase {
 public:
  TableBase(const std::string& nm, Pipeline* owner) : pipeline_(owner) { op_.set_op_name(nm); }

  TableBase(pb::Operator op, Pipeline* owner) : op_(std::move(op)), pipeline_(owner) {}

  virtual ~TableBase() {}

  virtual HandlerWrapperBase* CreateHandler(RawContext* context) = 0;

  pb::Operator CreateLink(bool from_output) const;

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
  std::function<HandlerWrapperBase*(RawContext* context)> handler_factory_;

  void SetOutput(const std::string& name, pb::WireFormat::Type type);
};

// Currently the input type is hard-coded - string.
template <typename OutT> class TableImpl : public TableBase {
 public:
  using OutputType = OutT;
  using ContextType = DoContext<OutT>;

  using PtrType = ::boost::intrusive_ptr<TableImpl<OutT>>;

  using TableBase::TableBase;  // C'tor

  Output<OutputType>& Write(const std::string& name, pb::WireFormat::Type type) {
    SetOutput(name, type);
    out_ = Output<OutputType>(op_.mutable_output());
    return out_;
  }

  template <typename U> typename TableImpl<U>::PtrType CloneAs(pb::Operator op) const {
    return new TableImpl<U>{std::move(op), pipeline_};
  }

  template <typename U> typename TableImpl<U>::PtrType CloneAs() const {
    auto new_op = op_;
    new_op.clear_output();
    return CloneAs<U>(std::move(new_op));
  }

  template <typename FromType, typename MapType> void MapWith();

  template <typename JoinerType> void JoinOn(
    std::initializer_list<HandlerBinding<JoinerType, OutT>> args);

  bool is_identity() const { return !handler_factory_; }

  HandlerWrapperBase* CreateHandler(RawContext* context) final;

 private:
  Output<OutT> out_;
};

template <typename Handler, typename ToType> class HandlerBinding {
 public:
  using SetupEmitFunc = std::function<RawSinkCb(Handler* handler, DoContext<ToType>* context)>;

  // This C'tor eliminates FromType and U and leaves common types (Joiner and ToType).
  template <typename FromType, typename U>
  HandlerBinding(const detail::TableImpl<FromType>* from, EmitMemberFn<U, Handler, ToType> ptr) {
    tbase_from = from;
    setup_func = [ptr](Handler* handler, DoContext<ToType>* context) {
      return [=, rt = RecordTraits<FromType>{}](RawRecord&& rr) mutable {
        detail::ParseAndDo(handler, &rt, context, ptr, std::move(rr));
      };
    };
  }

  const TableBase* tbase_from;
  SetupEmitFunc setup_func;
};

template <typename OutT> HandlerWrapperBase* TableImpl<OutT>::CreateHandler(RawContext* context) {
  if (handler_factory_) {
    return handler_factory_(context);
  }
  return new IdentityHandlerWrapper<OutT>(&out_, context);
}

template <typename OutT>
template <typename FromType, typename MapType>
void TableImpl<OutT>::MapWith() {
  handler_factory_ = [this](RawContext* raw_ctxt) {
    auto* ptr = new HandlerWrapper<MapType, OutT>(&out_, raw_ctxt);
    ptr->template Add<FromType>(&MapType::Do);

    return ptr;
  };
}

template <typename OutT>
template <typename JoinerType> void TableImpl<OutT>::JoinOn(
    std::initializer_list<detail::HandlerBinding<JoinerType, OutT>> args) {

}

}  // namespace detail
}  // namespace mr3
