// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "mr/impl/local_context.h"

namespace mr3 {

using namespace std;

namespace detail {

/// Thread-local buffered writer, owned by LocalContext.
class BufferedWriter {
  DestHandle* dh_;

 public:
  // dh not owned by BufferedWriter.
  BufferedWriter(DestHandle* dh, bool is_binary);

  BufferedWriter(const BufferedWriter&) = delete;
  ~BufferedWriter();

  void Flush();

  void Write(string&& val);

 private:
  static constexpr size_t kFlushLimit = 1 << 13;

  void operator=(const BufferedWriter&) = delete;

  bool is_binary_;

  string buffer_;
  vector<string> items_;
  size_t buffered_size_ = 0;

  size_t writes_ = 0, flushes_ = 0;
  DestHandle::StringGenCb str_cb_;
};

BufferedWriter::BufferedWriter(DestHandle* dh, bool is_binary) : dh_(dh), is_binary_(is_binary) {
  if (is_binary) {
    str_cb_ = [this]() -> absl::optional<std::string> {
      if (items_.empty()) {
        return absl::nullopt;
      }
      string val = std::move(items_.back());
      items_.pop_back();
      return val;
    };
  } else {
    str_cb_ = [this]() -> absl::optional<std::string> {
      return buffer_.empty() ? absl::nullopt : absl::optional<std::string>{std::move(buffer_)};
    };
  }
}

BufferedWriter::~BufferedWriter() {
  CHECK_EQ(0, buffered_size_);
}

void BufferedWriter::Flush() {
  if (buffered_size_) {
    dh_->Write(str_cb_);
    buffered_size_ = 0;
  }
}

void BufferedWriter::Write(string&& val) {
  buffered_size_ += (val.size() + 1);
  if (is_binary_) {
    items_.push_back(std::move(val));
  } else {
    buffer_.append(val).append("\n");
  }

  VLOG_IF(2, ++writes_ % 1000 == 0) << "BufferedWrite " << writes_;
  if (buffered_size_ >= kFlushLimit) {
    VLOG(2) << "Flush " << ++flushes_;

    dh_->Write(str_cb_);
    buffered_size_ = 0;
  }
}

LocalContext::LocalContext(DestFileSet* mgr) : mgr_(mgr) { CHECK(mgr_); }

void LocalContext::WriteInternal(const ShardId& shard_id, std::string&& record) {
  DCHECK(shard_id.is_defined()) << "Undefined shard id";

  auto it = custom_shard_files_.find(shard_id);
  if (it == custom_shard_files_.end()) {
    DestHandle* res = mgr_->GetOrCreate(shard_id);
    bool is_binary = mgr_->output().format().type() == pb::WireFormat::LST;
    it = custom_shard_files_.emplace(shard_id, new BufferedWriter{res, is_binary}).first;
  }
  it->second->Write(std::move(record));
}

void LocalContext::Flush() {
  for (auto& k_v : custom_shard_files_) {
    k_v.second->Flush();
  }
}

void LocalContext::CloseShard(const ShardId& shard_id) {
  auto it = custom_shard_files_.find(shard_id);
  if (it == custom_shard_files_.end()) {
    LOG(ERROR) << "Could not find shard " << shard_id.ToString("shard");
    return;
  }
  BufferedWriter* bw = it->second;
  bw->Flush();
  mgr_->CloseHandle(shard_id);
}

LocalContext::~LocalContext() {
  for (auto& k_v : custom_shard_files_) {
    delete k_v.second;
  }
  VLOG(1) << "~LocalContextEnd";
}

}  // namespace detail
}  // namespace mr3
