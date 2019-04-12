// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <functional>

#include "mr/output.h"

namespace mr3 {
class Pipeline;
class RawContext;
template <typename T> class DoContext;
template <typename Record> struct RecordTraits;

namespace detail {

class TableBase {
 public:
  using RawRecord = std::string;
  typedef std::function<void(RawRecord&& record)> DoFn;

  TableBase(const std::string& nm, Pipeline* owner) : pipeline_(owner) { op_.set_op_name(nm); }

  TableBase(pb::Operator op, Pipeline* owner) : op_(std::move(op)), pipeline_(owner) {}

  virtual ~TableBase() {}

  virtual DoFn SetupDoFn(RawContext* context) = 0;

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

  bool is_identity() const { return !do_fn_; }

 protected:
  DoFn SetupDoFn(RawContext* context) override;

 private:
  Output<OutT> out_;
  std::function<void(RawRecord&&, DoContext<OutputType>* context)> do_fn_;
};

template <typename OutT> auto TableImpl<OutT>::SetupDoFn(RawContext* context) -> DoFn {
  if (do_fn_) {
    return [f = this->do_fn_, wrapper = ContextType(&out_, context)](RawRecord&& r) mutable {
      f(std::move(r), &wrapper);
    };
  } else {
    return [wrapper = ContextType(&out_, context)](RawRecord&& r) mutable {
      OutT val;
      if (wrapper.ParseRaw(std::move(r), &val)) {
        wrapper.Write(std::move(val));
      }
    };
  }
}

template <typename OutT>
template <typename FromType, typename MapType>
void TableImpl<OutT>::MapWith() {
  struct Helper {
    MapType m;
    RecordTraits<FromType> rt;
  };

  do_fn_ = [h = Helper{}](RawRecord&& rr, DoContext<OutputType>* context) mutable {
    FromType tmp_rec;
    bool parse_res = context->raw_context()->ParseInto(std::move(rr), &h.rt, &tmp_rec);
    if (parse_res) {
      h.m.Do(std::move(tmp_rec), context);
    }
  };
}

}  // namespace detail
}  // namespace mr3
