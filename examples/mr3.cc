// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include <boost/fiber/buffered_channel.hpp>

#include "base/init.h"
#include "base/logging.h"

#include "file/fiber_file.h"
#include "file/file_util.h"
#include "file/filesource.h"
#include "mr/mr.h"

#include "util/asio/io_context_pool.h"
#include "util/status.h"

using namespace std;
using namespace boost;
using namespace util;

using fibers::channel_op_status;

DEFINE_string(input, "", "");

namespace mr3 {

StringStream& Pipeline::ReadText(const string& name, const string& glob) {
  std::unique_ptr<InputBase> ib(new InputBase(name, pb::WireFormat::TXT));
  ib->mutable_msg()->add_file_spec()->set_url_glob(glob);
  inputs_.emplace_back(std::move(ib));

  streams_.emplace_back(new StringStream(name));
  auto& ptr = streams_.back();

  return *ptr;
}

using util::Status;

Status Pipeline::Run() { return Status::OK; }

class MyDoContext : public DoContext {
 public:
  MyDoContext(const std::string& root_dir, const pb::Output& out);

  void Write(const ShardId& shard_id, const std::string& record) final;

 private:
  const std::string root_dir_;
  UniqueStrings str_db_;

  google::dense_hash_map<StringPiece, file::WriteFile*> custom_shard_files_;
  fibers::mutex mu_;
};

MyDoContext::MyDoContext(const std::string& root_dir, const pb::Output& out) : root_dir_(root_dir) {
  custom_shard_files_.set_empty_key(StringPiece{});
  CHECK(out.has_shard_type() && out.shard_type() == pb::Output::USER_DEFINED);
}

// TODO: To make MyDoContext thread local
void MyDoContext::Write(const ShardId& shard_id, const std::string& record) {
  std::lock_guard<fibers::mutex> lg(mu_);

  CHECK(absl::holds_alternative<string>(shard_id));

  const string& shard_name = absl::get<string>(shard_id);

  auto it = custom_shard_files_.find(shard_name);
  if (it == custom_shard_files_.end()) {
    LOG(FATAL) << "TBD";
  }
  file::WriteFile* fl = it->second;
  CHECK_STATUS(fl->Write(record));
}

class Executor {
  using StringQueue = ::boost::fibers::buffered_channel<string>;

  struct PerIoStruct {
    unsigned index;
    fibers::fiber process_fd, map_fd;
    StringQueue record_q;

    PerIoStruct(unsigned i);

    ~PerIoStruct();
  };

 public:
  Executor(const std::string& root_dir, util::IoContextPool* pool)
      : root_dir_(root_dir), pool_(pool), file_name_q_(16),
        fq_pool_(new util::FiberQueueThreadPool()) {}

  void Init();
  void Run(const InputBase* input, StringStream* ss);

 private:
  void ProcessFiles(pb::WireFormat::Type tp);
  void ProcessText(file::ReadonlyFile* fd);
  void MapFiber(StreamBase* sb, DoContext* cntx);

  std::string root_dir_;
  util::IoContextPool* pool_;
  StringQueue file_name_q_;
  static thread_local std::unique_ptr<PerIoStruct> per_io_;
  std::unique_ptr<util::FiberQueueThreadPool> fq_pool_;
};

thread_local std::unique_ptr<Executor::PerIoStruct> Executor::per_io_;

Executor::PerIoStruct::PerIoStruct(unsigned i) : index(i), record_q(32) {
}

Executor::PerIoStruct::~PerIoStruct() {
  DCHECK(record_q.is_closed());

  if (process_fd.joinable())
    process_fd.join();
  if (map_fd.joinable()) {
    map_fd.join();
  }
}

void Executor::Init() { CHECK(file_util::RecursivelyCreateDir(root_dir_, 0644)); }

void Executor::Run(const InputBase* input, StringStream* ss) {
  CHECK(input && input->msg().file_spec_size() > 0);
  CHECK(input->msg().has_format());
  LOG(INFO) << "Running on input " << input->msg().name();

  MyDoContext out_cntx(root_dir_, ss->output().msg());

  pool_->AwaitOnAll([&](unsigned index, IoContext&) {
    per_io_.reset(new PerIoStruct(index));
    per_io_->process_fd =
        fibers::fiber(&Executor::ProcessFiles, this, input->msg().format().type());
    per_io_->map_fd = fibers::fiber(&Executor::MapFiber, this, ss, &out_cntx);
  });

  for (const auto& file_spec : input->msg().file_spec()) {
    std::vector<file_util::StatShort> paths = file_util::StatFiles(file_spec.url_glob());
    for (const auto& v : paths) {
      if (v.st_mode & S_IFREG) {
        channel_op_status st = file_name_q_.push(v.name);
        CHECK_EQ(channel_op_status::success, st);
      }
    }
  }
}

void Executor::ProcessFiles(pb::WireFormat::Type input_type) {
  string file_name;

  while (true) {
    channel_op_status st = file_name_q_.pop(file_name);
    if (st == channel_op_status::closed)
      break;

    CHECK(st == channel_op_status::success);
    auto res = file::OpenFiberReadFile(file_name, fq_pool_.get());
    if (!res.ok()) {
      LOG(DFATAL) << "Skipping " << file_name << " with " << res.status;
      continue;
    }
    LOG(INFO) << "Processing file " << file_name;
    std::unique_ptr<file::ReadonlyFile> read_file(res.obj);

    switch (input_type) {
      case pb::WireFormat::TXT:
        ProcessText(read_file.get());
        break;

      default:
        LOG(FATAL) << "Not implemented " << pb::WireFormat::Type_Name(input_type);
        break;
    }
  }
  per_io_->record_q.close();
}

void Executor::ProcessText(file::ReadonlyFile* fd) {
  util::Source* src = file::Source::Uncompressed(fd);
  file::LineReader lr(src, TAKE_OWNERSHIP);
  StringPiece result;
  string scratch;

  while (lr.Next(&result, &scratch)) {
    channel_op_status st = per_io_->record_q.push(string(result));
    CHECK_EQ(channel_op_status::success, st);
  }
}

void Executor::MapFiber(StreamBase* sb, DoContext* cntx) {
  auto& record_q = per_io_->record_q;
  string record;
  while (true) {
    channel_op_status st = record_q.pop(record);
    if (st == channel_op_status::closed)
      break;

    CHECK(st == channel_op_status::success);

    // record is a binary input.
    // TODO: to implement binary to type to binary flow:
    // out_cntx-Deserialize<T>(record) -> T -> UDF(T) -> (Shard, U) -> Serialize(U)->string.
    // TODO: we should hold local map for sharded files.
    // if a new shard is needed, locks and accesses a central repository.
    // each sharded file is fiber-safe file.

    // We should have here Shard/string(out_record).
    sb->Do(std::move(record), cntx);
  }
}

}  // namespace mr3

using namespace mr3;
using namespace util;

int main(int argc, char** argv) {
  MainInitGuard guard(&argc, &argv);

  CHECK(!FLAGS_input.empty());

  IoContextPool pool;
  Pipeline p;

  pool.Run();

  Executor executor("/tmp/mr3", &pool);

  // TODO: Should return Input<string> or something which can apply an operator.
  StringStream& ss = p.ReadText("inp1", FLAGS_input);
  ss.Write("outp1", pb::WireFormat::TXT)
      .AndCompress(pb::Output::GZIP)
      .WithSharding([](const string& str) { return "shardname"; });

  executor.Run(&p.input("inp1"), &ss);

  return 0;
}
