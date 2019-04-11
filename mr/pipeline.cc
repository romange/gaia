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

  detail::TableImpl<std::string>::PtrType ptr(new detail::TableImpl<std::string>(name, this));
  ptr->mutable_op()->add_input_name(name);

  return StringTable{ptr};
}

void Pipeline::Stop() {
  if (executor_)
    executor_->Stop();
}

void Pipeline::Run(Runner* runner) {
  CHECK(!tables_.empty());

  executor_.reset(new Executor{pool_, runner});
  executor_->Init();

  for (auto ptr : tables_) {
    const pb::Operator& op = ptr->op();

    if (op.input_name_size() == 0) {
      LOG(INFO) << "No inputs for " << op.op_name() << ", skipping";
      continue;
    }

    // TODO: to move this to the planning phase.
    CHECK(!op.output().name().empty());
    auto inp_insert_res = inputs_.emplace(op.output().name(), nullptr);
    CHECK(inp_insert_res.second) << "Input '" << op.output().name() << "' already exists";
    auto& inp_ptr = inp_insert_res.first->second;
    inp_ptr.reset(new InputBase(op.output().name(), op.output().format().type()));

    std::vector<const InputBase*> inputs;

    for (const auto& input_name : op.input_name()) {
      inputs.push_back(CheckedInput(input_name));
    }

    std::vector<string> out_files;
    executor_->Run(inputs, ptr.get(), &out_files);
    VLOG(1) << "Executor finished running on " << op.op_name() << ", wrote to "
            << out_files.size() << " output files";
    for (const auto& fl : out_files) {
      auto* fs = inp_ptr->mutable_msg()->add_file_spec();
      fs->set_url_glob(fl);
    }
  }

  executor_->Shutdown();
}

Runner::~Runner() {}

}  // namespace mr3
