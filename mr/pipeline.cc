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

Pipeline::InputSpec::InputSpec(const std::vector<std::string>& globs) {
  for (const auto& s : globs) {
    pb::Input::FileSpec fspec;
    fspec.set_url_glob(s);
    file_spec_.push_back(std::move(fspec));
  }
}

Pipeline::Pipeline(IoContextPool* pool) : pool_(pool) {}
Pipeline::~Pipeline() {}

const InputBase* Pipeline::CheckedInput(const std::string& name) const {
  auto it = inputs_.find(name);
  CHECK(it != inputs_.end()) << "Could not find " << name;

  return it->second.get();
}

PInput<std::string> Pipeline::Read(const std::string& name, pb::WireFormat::Type format,
                                   const InputSpec& globs) {
  auto res = inputs_.emplace(name, nullptr);
  CHECK(res.second) << "Input " << name << " already exists";

  auto& inp_ptr = res.first->second;
  inp_ptr.reset(new InputBase(name, format));
  for (const auto& s : globs.file_spec()) {
    inp_ptr->mutable_msg()->add_file_spec()->CopyFrom(s);
  }

  pb::Operator op;
  op.set_op_name(name);
  op.add_input_name(name);

  using StringImpl = detail::TableImplT<string>;

  shared_ptr<StringImpl> ptr = StringImpl::AsRead(std::move(op), this);

  return PInput<std::string>(std::move(ptr), inp_ptr.get());
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

  for (const auto& sptr : tables_) {
    const pb::Operator& op = sptr->op();

    if (op.input_name_size() == 0) {
      LOG(INFO) << "No inputs for " << op.op_name() << ", skipping";
      continue;
    }

    if (stopped_) {
      break;
    }

    // We lock due to protect again Stop() breaks.
    std::unique_lock<fibers::mutex> lk(mu_);
    switch (op.type()) {
      case pb::Operator::GROUP:
        executor_ = std::make_shared<JoinerExecutor>(pool_, runner);
        break;
      default:
        executor_ = std::make_shared<MapperExecutor>(pool_, runner);
    }

    executor_->Init(freq_maps_);
    lk.unlock();
    ProcessTable(sptr.get());
  }

  VLOG(1) << "Before Runner::Shutdown";
  runner->Shutdown();
}

void Pipeline::ProcessTable(detail::TableBase* tbl) {
  const pb::Operator& op = tbl->op();
  std::vector<const InputBase*> inputs;
  string input_names;
  for (const auto& input_name : op.input_name()) {
    CHECK(!input_name.empty()) << "Empty input found for operator '" << op.op_name() << "'";

    absl::StrAppend(&input_names, input_name, ",");
    inputs.push_back(CheckedInput(input_name));
  }
  input_names.pop_back();

  // TODO: To allow skipping of the pipeline - i.e. partial dry run mode. For that we need to
  // scan output directory of each operator for shard files and populate shards from there.
  // In addition we must save freq maps on disk to allow loading them during dry run.
  LOG(INFO) << op.op_name() << " started on inputs [" << input_names << "]";
  ShardFileMap out_files;
  executor_->Run(inputs, tbl, &out_files);

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

  auto cb = [this](string k, FrequencyMap<uint32_t>* ptr) {
    auto res = freq_maps_.emplace(std::move(k), ptr);
    CHECK(res.second) << "Frequency map " << k
                      << " was created more than once across the pipeline run.";
  };

  executor_->ExtractFreqMap(cb);
}

pb::Input* Pipeline::mutable_input(const std::string& name) {
  auto it = inputs_.find(name);
  CHECK(it != inputs_.end());

  return it->second->mutable_msg();
}

const FrequencyMap<uint32_t>* Pipeline::GetFreqMap(const std::string& map_id) const {
  auto it = freq_maps_.find(map_id);
  if (it == freq_maps_.end())
    return nullptr;
  return it->second.get();
}

Runner::~Runner() {}

}  // namespace mr3
