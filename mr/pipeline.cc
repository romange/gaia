// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "mr/pipeline.h"
#include "mr/pipeline_executor.h"

#include "base/logging.h"

namespace mr3 {
using namespace std;
using namespace util;

Pipeline::Pipeline(IoContextPool* pool) : pool_(pool) {}
Pipeline::~Pipeline() {}

const InputBase* Pipeline::CheckedInput(const std::string& name) const {
  auto it = inputs_.find(name);
  CHECK(it != inputs_.end()) << "Could not find " << name;

  return it->second.get();
}

StringTable Pipeline::ReadText(const string& name, const std::vector<std::string>& globs) {
  auto res = inputs_.emplace(name, nullptr);
  CHECK(res.second) << "Input " << name << " already exists";

  auto& inp_ptr = res.first->second;
  inp_ptr.reset(new InputBase(name, pb::WireFormat::TXT));
  for (const auto& s : globs) {
    inp_ptr->mutable_msg()->add_file_spec()->set_url_glob(s);
  }

  TableImpl<std::string>::PtrType ptr(new TableImpl<std::string>(name, this));
  ptr->mutable_op()->add_input_name(name);

  return StringTable{ptr};
}

void Pipeline::Stop() {
  if (executor_)
    executor_->Stop();
}

void Pipeline::Run(Runner* runner) {
  CHECK(!tables_.empty());
  boost::intrusive_ptr<TableBase> ptr = tables_.front();

  const pb::Operator& op = ptr->op();

  if (op.input_name_size() == 0) {
    LOG(INFO) << "No inputs for " << op.op_name() << ", skipping";
    return;
  }

  executor_.reset(new Executor{pool_, runner});
  executor_->Init();

  std::vector<const InputBase*> inputs;


  for (const auto& input_name : op.input_name()) {
    inputs.push_back(CheckedInput(input_name));
  }

  executor_->Run(inputs, ptr.get());
  executor_->Shutdown();
}

Runner::~Runner() {
}

}  // namespace mr3
