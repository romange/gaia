// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <boost/fiber/mutex.hpp>
#include "mr/mr.h"

#include "absl/container/flat_hash_map.h"

namespace util {
class IoContextPool;
}  // namespace util

namespace mr3 {
class Runner;
class OperatorExecutor;

template <typename T> class PInput : public PTable<T> {
  friend class Pipeline;

  PInput(detail::TableBase* ptr, InputBase* ib)
      : PTable<T>(detail::TableImpl<T>::AsIdentity(ptr)), input_(ib) {}

 public:
  PInput<T>& set_skip_header(unsigned num_records) {
    input_->mutable_msg()->set_skip_header(num_records);
    return *this;
  }

 private:
  InputBase* input_;
};

class Pipeline {
  friend class detail::TableBase;

 public:
  explicit Pipeline(util::IoContextPool* pool);
  ~Pipeline();

  PInput<std::string> ReadText(const std::string& name, const std::vector<std::string>& globs);

  PInput<std::string> ReadText(const std::string& name, const std::string& glob) {
    return ReadText(name, std::vector<std::string>{glob});
  }

  void Run(Runner* runner);

  // Stops/breaks the run.
  void Stop();

  template <typename JoinerType, typename Out>
  PTable<Out> Join(const std::string& name,
                   std::initializer_list<detail::HandlerBinding<JoinerType, Out>> args);

  pb::Input* mutable_input(const std::string&);

 private:
  const InputBase* CheckedInput(const std::string& name) const;

  typename detail::TableBase* CreateTableImpl(const std::string& name) {
    return new detail::TableBase(name, this);
  }

  util::IoContextPool* pool_;
  absl::flat_hash_map<std::string, std::unique_ptr<InputBase>> inputs_;
  std::vector<detail::TableBase*> tables_;

  ::boost::fibers::mutex mu_;
  std::unique_ptr<OperatorExecutor> executor_;  // guarded by mu_
  std::atomic_bool stopped_{false};
};

template <typename JoinerType, typename OutT>
PTable<OutT> Pipeline::Join(const std::string& name,
                            std::initializer_list<detail::HandlerBinding<JoinerType, OutT>> args) {
  detail::TableBase* ptr = CreateTableImpl(name);
  std::for_each(args.begin(), args.end(), [&](const auto& arg) {
    ptr->mutable_op()->add_input_name(arg.tbase()->op().output().name());
  });
  ptr->mutable_op()->set_type(pb::Operator::HASH_JOIN);

  std::vector<RawSinkMethodFactory<JoinerType, OutT>> factories;
  for (auto& a : args) {
    factories.push_back(a.factory());
  }

  auto* res = detail::TableImpl<OutT>::template AsJoin<JoinerType>(ptr, std::move(factories));
  return PTable<OutT>{res};
}

template <typename U, typename Joiner, typename Out, typename S>
detail::HandlerBinding<Joiner, Out> JoinInput(const PTable<U>& tbl,
                                              EmitMemberFn<S, Joiner, Out> ptr) {
  return tbl.BindWith(ptr);
}

}  // namespace mr3
