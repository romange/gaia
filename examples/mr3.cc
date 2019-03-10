// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include <boost/fiber/buffered_channel.hpp>

#include "absl/strings/str_cat.h"

#include "base/init.h"
#include "base/logging.h"

#include "file/fiber_file.h"
#include "file/file_util.h"
#include "file/filesource.h"
#include "mr/mr.h"

#include "util/asio/accept_server.h"
#include "util/asio/io_context_pool.h"
#include "util/fibers/simple_channel.h"
#include "util/http/http_conn_handler.h"

#include "util/status.h"
#include "util/zlib_source.h"

using namespace std;
using namespace boost;
using namespace util;

using fibers::channel_op_status;

DEFINE_uint32(http_port, 8080, "Port number.");
DEFINE_uint32(mr_threads, 0, "Number of mr threads");

namespace mr3 {

class MyDoContext : public DoContext {
 public:
  MyDoContext(const std::string& root_dir, const pb::Output& out,
              fibers_ext::FiberQueueThreadPool* fq);
  ~MyDoContext();

  void Write(const ShardId& shard_id, std::string&& record) final;

 private:
  const std::string root_dir_;
  UniqueStrings str_db_;

  google::dense_hash_map<StringPiece, file::WriteFile*> custom_shard_files_;
  fibers::mutex mu_;
  fibers_ext::FiberQueueThreadPool* fq_;
};

MyDoContext::MyDoContext(const std::string& root_dir, const pb::Output& out,
                         fibers_ext::FiberQueueThreadPool* fq)
    : root_dir_(root_dir), fq_(fq) {
  custom_shard_files_.set_empty_key(StringPiece{});
  CHECK(out.has_shard_type() && out.shard_type() == pb::Output::USER_DEFINED);
}

// TODO: To make MyDoContext thread local
void MyDoContext::Write(const ShardId& shard_id, std::string&& record) {
  std::lock_guard<fibers::mutex> lg(mu_);

  CHECK(absl::holds_alternative<string>(shard_id));

  const string& shard_name = absl::get<string>(shard_id);

  auto it = custom_shard_files_.find(shard_name);
  if (it == custom_shard_files_.end()) {
    StringPiece key = str_db_.Get(shard_name);

    string file_name = absl::StrCat(shard_name, ".txt");
    string fn = file_util::JoinPath(root_dir_, file_name);
    auto res = custom_shard_files_.emplace(key, file::OpenFiberWriteFile(fn, fq_));
    CHECK(res.second && res.first->second);
    it = res.first;
  }
  file::WriteFile* fl = it->second;
  CHECK(fl);
  record.append("\n");
  CHECK_STATUS(fl->Write(record));
}

MyDoContext::~MyDoContext() {
  for (auto& k_v : custom_shard_files_) {
    CHECK(k_v.second->Close());
  }
}

class Executor {
  using StringQueue = fibers_ext::SimpleChannel<string>;
  using FileNameQueue = ::boost::fibers::buffered_channel<string>;

  struct PerIoStruct {
    unsigned index;
    fibers::fiber map_fd;
    StringQueue record_q;
    fibers_ext::Done process_done;

    PerIoStruct(unsigned i);

    void Shutdown();
  };

 public:
  Executor(const std::string& root_dir, util::IoContextPool* pool)
      : root_dir_(root_dir), pool_(pool), file_name_q_(16),
        fq_pool_(new fibers_ext::FiberQueueThreadPool()) {}
  ~Executor();

  void Init();
  void Run(const InputBase* input, StringStream* ss);
  void Shutdown();

 private:
   // External, disk thread that reads files from disk and pumps data into record_q.
   // One per IO thread.
  void ProcessFiles(pb::WireFormat::Type tp, PerIoStruct* record);
  uint64_t ProcessText(file::ReadonlyFile* fd, PerIoStruct* record);

  void MapFiber(StreamBase* sb, DoContext* cntx);

  std::string root_dir_;
  util::IoContextPool* pool_;
  FileNameQueue file_name_q_;
  static thread_local std::unique_ptr<PerIoStruct> per_io_;
  std::unique_ptr<fibers_ext::FiberQueueThreadPool> fq_pool_;
  std::unique_ptr<MyDoContext> my_context_;
};

thread_local std::unique_ptr<Executor::PerIoStruct> Executor::per_io_;

Executor::PerIoStruct::PerIoStruct(unsigned i) : index(i), record_q(256) {}

void Executor::PerIoStruct::Shutdown() {
  process_done.Wait();

  if (map_fd.joinable()) {
    map_fd.join();
  }
}

Executor::~Executor() {
  VLOG(1) << "Executor::~Executor";
  CHECK(file_name_q_.is_closed());
}

void Executor::Shutdown() {
  VLOG(1) << "Executor::Shutdown::Start";
  file_name_q_.close();
 
  // Use AwaitFiberOnAll because we block in the function.
  pool_->AwaitFiberOnAll([&](IoContext&) { per_io_->Shutdown(); });

  fq_pool_->Shutdown();
  VLOG(1) << "Executor::Shutdown";
}

void Executor::Init() { file_util::RecursivelyCreateDir(root_dir_, 0750); }

void Executor::Run(const InputBase* input, StringStream* ss) {
  CHECK(input && input->msg().file_spec_size() > 0);
  CHECK(input->msg().has_format());
  LOG(INFO) << "Running on input " << input->msg().name();

  my_context_.reset(new MyDoContext(root_dir_, ss->output().msg(), fq_pool_.get()));

  using sa_traits = fibers::protected_fixedsize_stack::traits_type;
  LOG(INFO) << "fss traits: " << sa_traits::default_size() << "/" << sa_traits::is_unbounded()
            << "/" << sa_traits::page_size();

  // As long as we do not block in the function we can use AwaitOnAll.
  pool_->AwaitOnAll([&](unsigned index, IoContext&) {
    per_io_.reset(new PerIoStruct(index));
    /*std::thread{&Executor::ProcessFiles, this, input->msg().format().type(), per_io_.get()}
        .detach();*/
    fibers::fiber{&Executor::ProcessFiles, this, input->msg().format().type(), per_io_.get()}
        .detach();
    per_io_->map_fd = fibers::fiber(&Executor::MapFiber, this, ss, my_context_.get());
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

void Executor::ProcessFiles(pb::WireFormat::Type input_type, PerIoStruct* rec) {
  string file_name;
  uint64_t cnt = 0;
  while (true) {
    channel_op_status st = file_name_q_.pop(file_name);
    if (st == channel_op_status::closed)
      break;

    CHECK_EQ(channel_op_status::success, st);
    // auto res = file::ReadonlyFile::Open(file_name);
    auto res = file::OpenFiberReadFile(file_name, fq_pool_.get());
    if (!res.ok()) {
      LOG(DFATAL) << "Skipping " << file_name << " with " << res.status;
      continue;
    }
    LOG(INFO) << "Processing file " << file_name;
    std::unique_ptr<file::ReadonlyFile> read_file(res.obj);

    switch (input_type) {
      case pb::WireFormat::TXT:
        cnt += ProcessText(read_file.release(), rec);
        break;

      default:
        LOG(FATAL) << "Not implemented " << pb::WireFormat::Type_Name(input_type);
        break;
    }
  }
  VLOG(1) << "ProcessFiles closing after processing " << cnt << " items";
  rec->record_q.StartClosing();
  rec->process_done.Notify();
  // VLOG(1) << "T1/T2: " << rec->record_q.t1 << "/" << rec->record_q.t2;
}

uint64_t Executor::ProcessText(file::ReadonlyFile* fd, PerIoStruct* record) {
  std::unique_ptr<util::Source> src(file::Source::Uncompressed(fd));
  /*constexpr size_t kBufSize = 1 << 15;
  std::unique_ptr<uint8_t[]> buf(new uint8_t[kBufSize]);
  strings::MutableByteRange mbr(buf.get(), kBufSize);

  while (true) {
    auto res = src->Read(mbr);
    CHECK_STATUS(res.status);
    if (res.obj < mbr.size())
      break;
  }*/

  file::LineReader lr(src.release(), TAKE_OWNERSHIP);
  StringPiece result;
  string scratch;
  uint64_t cnt = 0;
  while (lr.Next(&result, &scratch)) {
    string tmp{result};
    ++cnt;
    record->record_q.Push(std::move(tmp));
  }
  return cnt;
}

void Executor::MapFiber(StreamBase* sb, DoContext* cntx) {
  auto& record_q = per_io_->record_q;
  string record;
  uint64_t record_num = 0;
  while (true) {
    bool is_open = record_q.Pop(record);
    if (!is_open)
      break;

    // record is a binary input.
    // TODO: to implement binary to type to binary flow:
    // out_cntx-Deserialize<T>(record) -> T -> UDF(T) -> (Shard, U) -> Serialize(U)->string.
    // TODO: we should hold local map for sharded files.
    // if a new shard is needed, locks and accesses a central repository.
    // each sharded file is fiber-safe file.

    // We should have here Shard/string(out_record).
    // sb->Do(std::move(record), cntx);
    if (++record_num % 1000 == 0) {
      this_fiber::yield();
    }
  }
  VLOG(1) << "MapFiber finished " << record_num;
}

}  // namespace mr3

using namespace mr3;
using namespace util;

string ShardNameFunc(const std::string& line) {
  char buf[32];
  snprintf(buf, sizeof buf, "shard-%04d", base::Fingerprint32(line) % 10);
  return buf;
}

int main(int argc, char** argv) {
  MainInitGuard guard(&argc, &argv);

  std::vector<string> inputs;
  for (int i = 1; i < argc; ++i) {
    inputs.push_back(argv[i]);
  }
  CHECK(!inputs.empty());

  IoContextPool pool(FLAGS_mr_threads);
  pool.Run();

  std::unique_ptr<util::AcceptServer> server(new AcceptServer(&pool));
  util::http::Listener<> http_listener;
  uint16_t port = server->AddListener(FLAGS_http_port, &http_listener);
  LOG(INFO) << "Started http server on port " << port;
  server->Run();

  Pipeline p;

  Executor executor("/tmp/mr3", &pool);
  executor.Init();

  // TODO: Should return Input<string> or something which can apply an operator.
  StringStream& ss = p.ReadText("inp1", inputs);
  ss.Write("outp1", pb::WireFormat::TXT).AndCompress(pb::Output::GZIP).WithSharding(ShardNameFunc);

  executor.Run(&p.input("inp1"), &ss);
  executor.Shutdown();

  server->Stop(true);

  return 0;
}
