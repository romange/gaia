// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include "mr/mr.h"

#include "absl/container/flat_hash_map.h"

namespace util {
class IoContextPool;
}  // namespace util

namespace mr3 {
class Runner;
class OperatorExecutor;

template <typename Joiner, typename Out> class JoinArg {
 public:
  using RawRecord = detail::TableBase::RawRecord;

  template <typename U> using FunctionPtr = void (Joiner::*)(U&&, DoContext<Out>*);
  using EmitFunc = std::function<void(RawRecord&&, DoContext<Out>* context)>;
  using SetupEmitFunc = std::function<EmitFunc(Joiner* joiner)>;

  template <typename U> JoinArg(const PTable<U>& tbl, FunctionPtr<U> ptr) {
    setup_func = [ptr](Joiner* joiner) {
      auto f = [ptr, joiner, rt = RecordTraits<U>{}](RawRecord&& rr,
                                                     DoContext<Out>* context) mutable {
        U tmp_rec;
        bool parse_res = context->raw_context()->ParseInto(std::move(rr), &rt, &tmp_rec);
        if (parse_res) {
          ((*joiner).*ptr)(std::move(tmp_rec), context);
        }
      };
      return f;
    };
  }

  SetupEmitFunc setup_func;
};

template <typename Joiner, typename Out, typename U>
JoinArg<Joiner, Out> JoinInput(const PTable<U>& tbl, void (Joiner::*fnc)(U&&, DoContext<Out>*)) {
  return JoinArg<Joiner, Out>{tbl, fnc};
}

class Pipeline {
  friend class detail::TableBase;

 public:
  explicit Pipeline(util::IoContextPool* pool);
  ~Pipeline();

  StringTable ReadText(const std::string& name, const std::vector<std::string>& globs);

  StringTable ReadText(const std::string& name, const std::string& glob) {
    return ReadText(name, std::vector<std::string>{glob});
  }

  void Run(Runner* runner);

  // Stops/breaks the run.
  void Stop();

  template <typename JoinerType, typename Out>
  PTable<Out> Join(const std::string& name, std::initializer_list<JoinArg<JoinerType, Out>> args);

 private:
  const InputBase* CheckedInput(const std::string& name) const;

  template <typename U>
  typename detail::TableImpl<U>::PtrType CreateTableImpl(const std::string& name) {
    return new detail::TableImpl<U>(name, this);
  }

  util::IoContextPool* pool_;
  absl::flat_hash_map<std::string, std::unique_ptr<InputBase>> inputs_;
  std::vector<boost::intrusive_ptr<detail::TableBase>> tables_;

  ::boost::fibers::mutex mu_;
  std::unique_ptr<OperatorExecutor> executor_;
};

template <typename JoinerType, typename Out>
PTable<Out> Pipeline::Join(const std::string& name,
                           std::initializer_list<JoinArg<JoinerType, Out>> args) {
  auto ptr = CreateTableImpl<Out>(name);
  return PTable<Out>(ptr);
}

}  // namespace mr3
