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

  detail::TableImpl<std::string>::PtrType ptr = CreateTableImpl<string>(name);
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

    std::vector<const InputBase*> inputs;

    for (const auto& input_name : op.input_name()) {
      inputs.push_back(CheckedInput(input_name));
    }

    ShardFileMap out_files;
    executor_->Run(inputs, ptr.get(), &out_files);

    VLOG(1) << "Executor finished running on " << op.op_name() << ", wrote to "
            << out_files.size() << " output files";

    // Fill the corresponsing input with sharded files.
    auto it = inputs_.find(op.output().name());
    CHECK(it != inputs_.end());
    auto& inp_ptr = it->second;

    for (const auto& k_v : out_files) {
      auto* fs = inp_ptr->mutable_msg()->add_file_spec();
      fs->set_url_glob(k_v.second);
      if (absl::holds_alternative<uint32_t>(k_v.first)) {
        fs->set_shard_id(absl::get<uint32_t>(k_v.first));
      } else {
        fs->set_custom_shard_id(absl::get<string>(k_v.first));
      }
    }
  }

  executor_->Shutdown();
}

Runner::~Runner() {}

}  // namespace mr3
