// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "mr/pipeline.h"
#include "mr/joiner_executor.h"
#include "mr/mapper_executor.h"

#include "base/logging.h"

namespace mr3 {
using namespace boost;
using namespace std;
using namespace util;

Pipeline::Pipeline(IoContextPool* pool) : pool_(pool) {}
Pipeline::~Pipeline() {}

const InputBase* Pipeline::CheckedInput(const std::string& name) const {
  auto it = inputs_.find(name);
  CHECK(it != inputs_.end()) << "Could not find " << name;

  return it->second.get();
}

PInput<std::string> Pipeline::ReadText(const string& name, const std::vector<std::string>& globs) {
  auto res = inputs_.emplace(name, nullptr);
  CHECK(res.second) << "Input " << name << " already exists";

  auto& inp_ptr = res.first->second;
  inp_ptr.reset(new InputBase(name, pb::WireFormat::TXT));
  for (const auto& s : globs) {
    inp_ptr->mutable_msg()->add_file_spec()->set_url_glob(s);
  }

  detail::TableBase* ptr = CreateTableImpl(name);
  ptr->mutable_op()->add_input_name(name);

  return PInput<std::string>(ptr, inp_ptr.get());
}

void Pipeline::Stop() {
  stopped_ = true;
  LOG(INFO) << "Breaking the run";

  std::lock_guard<fibers::mutex> lk(mu_);

  if (executor_)
    executor_->Stop();
}

void Pipeline::Run(Runner* runner) {
  CHECK(!tables_.empty());

  for (auto ptr : tables_) {
    const pb::Operator& op = ptr->op();

    if (op.input_name_size() == 0) {
      LOG(INFO) << "No inputs for " << op.op_name() << ", skipping";
      continue;
    }

    if (stopped_) {
      break;
    }

    std::unique_lock<fibers::mutex> lk(mu_);
    switch (op.type()) {
      case pb::Operator::HASH_JOIN:
        executor_.reset(new JoinerExecutor{pool_, runner});
        break;
      default:
        executor_.reset(new MapperExecutor{pool_, runner});
    }

    executor_->Init();
    lk.unlock();

    std::vector<const InputBase*> inputs;
    string input_names;
    for (const auto& input_name : op.input_name()) {
      absl::StrAppend(&input_names, input_name, ",");
      inputs.push_back(CheckedInput(input_name));
    }
    input_names.pop_back();

    LOG(INFO) << op.op_name() << " started on inputs [" << input_names << "]";
    ShardFileMap out_files;
    executor_->Run(inputs, ptr, &out_files);
    LOG(INFO) << op.op_name() << " finished run with " << out_files.size() << " output files";

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

  VLOG(1) << "Before Runner::Shutdown";
  runner->Shutdown();
}

pb::Input* Pipeline::mutable_input(const std::string& name) {
  auto it = inputs_.find(name);
  CHECK(it != inputs_.end());

  return it->second->mutable_msg();
}

Runner::~Runner() {}

}  // namespace mr3
