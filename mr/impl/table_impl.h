// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <functional>

#include "base/type_traits.h"
#include "mr/do_context.h"

namespace mr3 {
class Pipeline;
class RawContext;

template <typename, typename> struct RecordTraits;

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

// int serves as a dummy argument to create overload precedence.
template <typename Handler, typename ToType>
base::void_t<decltype(&Handler::OnShardFinish)> FinishCallMaybe(Handler* h, DoContext<ToType>* cntx,
                                                                int) {
  h->OnShardFinish(cntx);
};

// 2nd priority overload when passing 0 intp char.
template <typename Handler, typename ToType>
void FinishCallMaybe(Handler* h, DoContext<ToType>* cntx, char){};

class HandlerWrapperBase {
 public:
  virtual ~HandlerWrapperBase() {}

  RawSinkCb Get(size_t index) const { return raw_fn_vec_[index]; }

  size_t Size() const { return raw_fn_vec_.size(); }

  virtual void SetOutputShard(ShardId sid) = 0;
  virtual void OnShardFinish() {}

 protected:
  template <typename F> void AddFn(F&& f) { raw_fn_vec_.emplace_back(std::forward<F>(f)); }

 private:
  std::vector<RawSinkCb> raw_fn_vec_;
};

template <typename T> class DefaultParser {
  RecordTraits<T> rt_;
  static_assert(std::is_copy_constructible<RecordTraits<T>>::value,
                "RecordTraits must be copyable");
  static_assert(std::is_default_constructible<RecordTraits<T>>::value,
                "RecordTraits must be default constractable");

 public:
  bool operator()(bool is_binary, RawRecord&& rr, T* res) {
    return rt_.Parse(is_binary, std::move(rr), res);
  }
};

template <typename FromType, typename Parser, typename DoFn, typename ToType>
void ParseAndDo(bool is_binary, Parser* parser, DoContext<ToType>* context, DoFn&& do_fn,
                RawRecord&& rr) {
  FromType tmp_rec;
  bool parse_ok = (*parser)(is_binary, std::move(rr), &tmp_rec);

  if (parse_ok) {
    do_fn(std::move(tmp_rec), context);
  } else {
    context->raw()->EmitParseError();
  }
}

template <typename Handler, typename ToType> class HandlerWrapper : public HandlerWrapperBase {
  Handler h_;
  DoContext<ToType> do_ctx_;

 public:
  template<typename... Args>
    HandlerWrapper(const Output<ToType>& out, RawContext* raw_context, Args&&... args)
     : h_(std::forward<Args>(args)...), do_ctx_(out, raw_context) {}

  void SetOutputShard(ShardId sid) final { do_ctx_.SetConstantShard(sid); }

  // We pass 0 into 3rd argument so compiler will prefer 'int' resolution if possible.
  void OnShardFinish() final { FinishCallMaybe(&h_, &do_ctx_, 0); }

  template <typename FromType, typename F> void Add(void (Handler::*ptr)(F, DoContext<ToType>*)) {
    AddFn([this, ptr, parser = DefaultParser<FromType>{}](bool is_binary, RawRecord&& rr) mutable {
      ParseAndDo<FromType>(is_binary, &parser, &do_ctx_,
                           [this, ptr](FromType&& val, DoContext<ToType>* cntx) {
                             return (h_.*ptr)(std::move(val), cntx);
                           },
                           std::move(rr));
    });
  }

  void AddFromFactory(const RawSinkMethodFactory<Handler, ToType>& f) { AddFn(f(&h_, &do_ctx_)); }
};

template <typename T, typename Parser = DefaultParser<T>>
class IdentityHandlerWrapper : public HandlerWrapperBase {
  DoContext<T> do_ctx_;
  Parser parser_;

 public:
  IdentityHandlerWrapper(const Output<T>& out, Parser parser, RawContext* raw_context)
      : do_ctx_(out, raw_context), parser_(std::move(parser)) {
    AddFn([this](bool is_binary, RawRecord&& rr) {
      T val;
      if (parser_(is_binary, std::move(rr), &val)) {
        do_ctx_.Write(std::move(val));
      } else {
        do_ctx_.raw()->EmitParseError();
      }
    });
  }

  void SetOutputShard(ShardId sid) final { do_ctx_.SetConstantShard(sid); }
};

class TableBase {
 public:
  TableBase(const std::string& nm, Pipeline* owner) : pipeline_(owner) { op_.set_op_name(nm); }

  TableBase(pb::Operator op, Pipeline* owner) : op_(std::move(op)), pipeline_(owner) {}

  const pb::Operator& op() const { return op_; }
  pb::Operator* mutable_op() { return &op_; }

  Pipeline* pipeline() const { return pipeline_; }

  void SetOutput(const std::string& name, pb::WireFormat::Type type);

  template <typename F> void SetHandlerFactory(F&& f) {
    handler_factory_ = std::forward<F>(f);
    is_identity_ = false;
  }

  template <typename OutT, typename Parser>
  void SetIdentity(const Output<OutT>* outp, Parser parser) {
    handler_factory_ = [outp, parser = std::move(parser)](RawContext* raw_ctxt) {
      return new IdentityHandlerWrapper<OutT, Parser>(*outp, parser, raw_ctxt);
    };
    is_identity_ = true;
  }

  TableBase* Clone() { return new TableBase(op_, pipeline_); }
  HandlerWrapperBase* CreateHandler(RawContext* context);

  TableBase* MappedTableFromMe(const std::string& name) const;
  void CheckFailIdentity();

 private:
  TableBase(const TableBase&) = delete;
  void operator=(const TableBase&) = delete;

  pb::Operator GetDependeeOp() const;

  bool is_identity() const { return is_identity_; }
  bool defined() const { return bool(handler_factory_); }

  pb::Operator op_;
  Pipeline* pipeline_;

  std::function<HandlerWrapperBase*(RawContext* context)> handler_factory_;
  bool is_identity_ = true;
};

template <typename Handler, typename ToType> class HandlerBinding {
  HandlerBinding(const TableBase* from) : tbase_(from) {}

 public:
  // This C'tor eliminates FromType and U and leaves common types (Joiner and ToType).
  template <typename FromType, typename U>
  static HandlerBinding<Handler, ToType> Create(const TableBase* from,
                                                EmitMemberFn<U, Handler, ToType> ptr) {
    HandlerBinding<Handler, ToType> res(from);
    res.setup_func_ = [ptr](Handler* handler, DoContext<ToType>* context) {
      auto do_fn = [handler, ptr](FromType&& val, DoContext<ToType>* cntx) {
        return (handler->*ptr)(std::move(val), cntx);
      };
      return [do_fn, context, parser = DefaultParser<FromType>{}](
		bool is_binary, RawRecord&& rr) mutable {
        ParseAndDo<FromType>(is_binary, &parser, context, std::move(do_fn), std::move(rr));
      };
    };
    return res;
  }

  const TableBase* tbase() const { return tbase_; }
  RawSinkMethodFactory<Handler, ToType> factory() const { return setup_func_; }

 private:
  const TableBase* tbase_;
  RawSinkMethodFactory<Handler, ToType> setup_func_;
};

// I need this class because I bind TableBase functions to output object contained in the class.
// Therefore TableBase and Output must be moved together.
template <typename OutT> class TableImpl {
  // apparently classes of different types can not access own members.
  template <typename T> friend class TableImpl;

 public:
  Output<OutT>& Write(const std::string& name, pb::WireFormat::Type type) {
    table_->SetOutput(name, type);
    output_ = Output<OutT>{table_->mutable_op()->mutable_output()};
    return output_;
  }

  // Identity Factory
  static TableImpl<OutT>* AsIdentity(TableBase* tb) {
    TableImpl* res = new TableImpl(tb);
    tb->SetIdentity(&res->output_, DefaultParser<OutT>{});
    return res;
  }

  // Map factory
  template <typename MapType, typename FromType, typename... Args>
  static TableImpl<OutT>* AsMapFrom(const std::string& name, const TableImpl<FromType>* ptr,
                                    Args&&... args) {
    TableBase* new_tb = ptr->table_->MappedTableFromMe(name);
    TableImpl* res = new TableImpl(new_tb);
    new_tb->SetHandlerFactory([res, args...](RawContext* raw_ctxt) {
      auto* ptr = new HandlerWrapper<MapType, OutT>(res->output_, raw_ctxt, args...);
      ptr->template Add<FromType>(&MapType::Do);
      return ptr;
    });

    return res;
  }

  template <typename U> TableImpl<U>* As() const {
    table_->CheckFailIdentity();
    return TableImpl<U>::AsIdentity(table_->Clone());
  }

  template <typename Handler, typename ToType, typename U>
  HandlerBinding<Handler, ToType> BindWith(EmitMemberFn<U, Handler, ToType> ptr) const {
    return HandlerBinding<Handler, ToType>::template Create<OutT>(table_.get(), ptr);
  }

  template <typename JoinerType>
  static TableImpl<OutT>* AsJoin(TableBase* ptr,
                                 std::vector<RawSinkMethodFactory<JoinerType, OutT>> factories) {
    TableImpl* res = new TableImpl(ptr);
    ptr->SetHandlerFactory([res, factories = std::move(factories)](RawContext* raw_ctxt) {
      auto* ptr = new HandlerWrapper<JoinerType, OutT>(res->output_, raw_ctxt);
      for (const auto& m : factories) {
        ptr->AddFromFactory(m);
      }
      return ptr;
    });
    return res;
  }

 private:
  TableImpl(TableBase* tb) : table_(tb) {}

  Output<OutT> output_;
  std::unique_ptr<TableBase> table_;
};

}  // namespace detail
}  // namespace mr3
