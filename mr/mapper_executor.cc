// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "mr/mapper_executor.h"

#include "absl/strings/str_cat.h"
#include "base/histogram.h"
#include "base/logging.h"
#include "base/walltime.h"
#include "mr/impl/table_impl.h"
#include "mr/ptable.h"

#include "util/asio/io_context_pool.h"
#include "util/fibers/fibers_ext.h"
#include "util/stats/varz_stats.h"

namespace mr3 {

DEFINE_uint32(map_limit, 0, "");
DEFINE_uint32(map_io_read_factor, 2, "");

using namespace std;
using namespace boost;
using namespace util;

using fibers::channel_op_status;

struct MapperExecutor::PerIoStruct {
  unsigned index;
  std::vector<::boost::fibers::fiber> process_fd;
  std::unique_ptr<RawContext> raw_context;
  size_t records_read = 0;
  absl::flat_hash_map<string, uint64_t> inputs_count_map;

  bool stop_early = false;

  PerIoStruct(unsigned i);

  void Shutdown();
};

MapperExecutor::PerIoStruct::PerIoStruct(unsigned i) : index(i) {
}

thread_local std::unique_ptr<MapperExecutor::PerIoStruct> MapperExecutor::per_io_;

void MapperExecutor::PerIoStruct::Shutdown() {
  VLOG(1) << "PerIoStruct::ShutdownStart";
  for (auto& f : process_fd)
    f.join();
  VLOG(1) << "PerIoStruct::ShutdownEnd";
}

MapperExecutor::MapperExecutor(util::IoContextPool* pool, Runner* runner)
    : OperatorExecutor(pool, runner) {
}

MapperExecutor::~MapperExecutor() {
  CHECK(!file_name_q_);
}

void MapperExecutor::InitInternal() {
  runner_->Init();
}

void MapperExecutor::Stop() {
  VLOG(1) << "MapperExecutor Stop[";

  // small race condition at the end, not important since this function called only on SIGTERM
  if (file_name_q_) {
    file_name_q_->close();
    pool_->AwaitOnAll([&](IoContext&) {
      if (per_io_) {  // "file_name_q_->close();"" might cause per_io be already freed.
        per_io_->stop_early = true;
      }
      VLOG(1) << "StopEarly";
    });
  }
  VLOG(1) << "MapperExecutor Stop]";
}

void MapperExecutor::SetupPerIoThread(unsigned index, detail::TableBase* tb) {
  auto* ptr = new PerIoStruct(index);
  ptr->raw_context.reset(runner_->CreateContext());
  RegisterContext(ptr->raw_context.get());

  per_io_.reset(ptr);

  CHECK_GT(FLAGS_map_io_read_factor, 0);
  per_io_->process_fd.resize(FLAGS_map_io_read_factor);

  for (auto& fbr : per_io_->process_fd) {
    fbr = fibers::fiber{&MapperExecutor::IOReadFiber, this, tb};
  }
}

void MapperExecutor::Run(const std::vector<const InputBase*>& inputs, detail::TableBase* tb,
                         ShardFileMap* out_files) {
  const string& op_name = tb->op().op_name();

  util::VarzFunction varz_func("mapper-executor", [this] { return GetStats(); });

  file_name_q_.reset(new FileNameQueue{16});
  runner_->OperatorStart(&tb->op());

  // As long as we do not block in the function we can use AwaitOnAll.
  pool_->AwaitOnAll([&](unsigned index, IoContext&) { SetupPerIoThread(index, tb); });

  for (const auto& input : inputs) {
    PushInput(input);

    if (file_name_q_->is_closed())
      break;
  }

  file_name_q_->close();

  // Use AwaitFiberOnAll because Shutdown() blocks the callback.
  pool_->AwaitFiberOnAll([&](IoContext&) {
    per_io_->Shutdown();
    FinalizeContext(per_io_->records_read, per_io_->raw_context.get());
    per_io_.reset();
  });

  LOG_IF(WARNING, parse_errors_ > 0) << op_name << " had " << parse_errors_.load() << " errors";
  for (const auto& k_v : metric_map_) {
    LOG(INFO) << op_name << "-" << k_v.first << ": " << k_v.second;
  }

  runner_->OperatorEnd(metric_map_, out_files);
  file_name_q_.reset();
}

void MapperExecutor::PushInput(const InputBase* input) {
  CHECK(input && input->msg().file_spec_size() > 0);
  CHECK(input->msg().has_format());

  vector<FileInput> files;
  pool_->GetNextContext().AwaitSafe([&] {
    const pb::Input* pb_input = &input->msg();
    for (int i = 0; i < pb_input->file_spec_size(); ++i) {
      const pb::Input::FileSpec& file_spec = pb_input->file_spec(i);
      runner_->ExpandGlob(file_spec.url_glob(), [&](size_t sz, const auto& str) {
        files.push_back(FileInput{pb_input, size_t(i), sz, str});
      });
    }
  });

  // Sort - bigger sizes first to reduce the variance of the reading phase.
  std::sort(files.begin(), files.end(),
            [](const auto& l, auto& r) { return l.file_size > r.file_size; });

  LOG(INFO) << "Running on input " << input->msg().name() << " with " << files.size() << " files";
  for (const auto& fl_name : files) {
    channel_op_status st = file_name_q_->push(fl_name);
    if (st != channel_op_status::closed) {
      CHECK_EQ(channel_op_status::success, st);
    }
  }
}

void MapperExecutor::IOReadFiber(detail::TableBase* tb) {
  this_fiber::properties<IoFiberProperties>().set_name("IOReadFiber");

  PerIoStruct* aux_local = per_io_.get();
  FileInput file_input;
  uint64_t cnt = 0;

  std::unique_ptr<detail::HandlerWrapperBase> handler{
      tb->CreateHandler(aux_local->raw_context.get())};
  CHECK_EQ(1, handler->Size());

  // contains items pushed from the IORead fiber but not yet processed by MapFiber.
  RecordQueue record_q(256);

  fibers::fiber map_fd(&MapperExecutor::MapFiber, &record_q, handler.get());

  VLOG(1) << "Starting MapFiber on " << tb->op().output().DebugString();

  while (!aux_local->stop_early) {
    channel_op_status st = file_name_q_->pop(file_input);
    if (st == channel_op_status::closed)
      break;

    CHECK_EQ(channel_op_status::success, st);
    const pb::Input* pb_input = file_input.input;
    pb::WireFormat::Type input_type = pb_input->format().type();
    bool is_binary = detail::IsBinary(input_type);
    Record::Operand op = is_binary ? Record::BINARY_FORMAT : Record::TEXT_FORMAT;
    record_q.Push(op, 0, file_input.file_name);
    record_q.Push(Record::METADATA, &pb_input->file_spec(file_input.spec_index));

    auto cb = [&, skip = pb_input->skip_header(),
               file_record_cnt = uint64_t{0}](string&& s) mutable {
      if (file_record_cnt++ < skip)
        return;
      record_q.Push(Record::RECORD, aux_local->records_read, std::move(s));
      ++aux_local->records_read;
    };

    size_t records_read =
        runner_->ProcessInputFile(file_input.file_name, input_type, std::move(cb));

    cnt += records_read;
    aux_local->inputs_count_map[pb_input->name()] += records_read;
  }
  VLOG(1) << "IOReadFiber closing after processing " << cnt << " items";

  // Must follow process_fd because we need first to push all the records to the queue and then
  // to signal it's closing.
  record_q.StartClosing();

  map_fd.join();
  handler->OnShardFinish();

  VLOG(1) << "IOReadFiber after OnShardFinish";
}

void MapperExecutor::MapFiber(RecordQueue* record_q, detail::HandlerWrapperBase* handler_wrapper) {
  auto& props = this_fiber::properties<IoFiberProperties>();
  props.set_name("MapFiber");
  props.SetNiceLevel(IoFiberProperties::MAX_NICE_LEVEL);

  PerIoStruct* aux_local = per_io_.get();
  RawContext* raw_context = aux_local->raw_context.get();
  CHECK(raw_context);

  Record record;
  uint64_t record_num = 0;
  RawSinkCb cb = handler_wrapper->Get(0);
  base::Histogram hist;

  while (true) {
    bool is_open = record_q->Pop(record);
    if (!is_open)
      break;

    if (record.op != Record::RECORD) {
      switch (record.op) {
        case Record::BINARY_FORMAT: {
          auto* rec = absl::get_if<pair<size_t, string>>(&record.payload);
          CHECK(rec);
          SetFileName(true, rec->second, raw_context);
          break;
        }
        case Record::TEXT_FORMAT: {
          auto* rec = absl::get_if<pair<size_t, string>>(&record.payload);
          CHECK(rec);
          SetFileName(false, rec->second, raw_context);
          break;
        }
        case Record::METADATA:
          SetMetaData(*absl::get<const pb::Input::FileSpec*>(record.payload), raw_context);
          break;

        case Record::UNDEFINED:
        case Record::RECORD:
          LOG(FATAL) << "Should not happen: " << record.op;
      }

      continue;
    }

    ++record_num;

    auto now = base::GetMonotonicMicrosFast();
    if (record_num % 100 == 0) {
      VLOG_IF(1, now - props.resume_ts() >= 100000) << "MapFiber CallStats: " << hist.ToString();

      hist.Clear();
      this_fiber::yield();
    }

    // TODO: to pass it as argument to Runner::ProcessInputFile.
    if (FLAGS_map_limit && record_num > FLAGS_map_limit) {
      continue;
    }

    VLOG_IF(1, record_num % 1000 == 0) << "Num maps " << record_num;

    auto& pos_payload = absl::get<pair<size_t, string>>(record.payload);
    SetPosition(pos_payload.first, raw_context);

    cb(std::move(pos_payload.second));
    if (VLOG_IS_ON(1)) {
      auto delta = base::GetMonotonicMicrosFast() - now;
      hist.Add(delta);
    }
  }
  VLOG(1) << "MapFiber finished " << record_num;
}

VarzValue::Map MapperExecutor::GetStats() const {
  VarzValue::Map res;
  atomic<size_t> parse_errors{0}, record_read{0};

  LOG(INFO) << "MapperExecutor::GetStats";

  auto start = base::GetMonotonicMicrosFast();
  absl::flat_hash_map<string, uint64_t> input_read;
  fibers::mutex mu;

  pool_->AwaitOnAll([&, me = shared_from_this()](IoContext& io) {
    VLOG(1) << "MapperExecutor::GetStats CB";
    auto delta = base::GetMonotonicMicrosFast() - start;
    LOG_IF(INFO, delta > 10000) << "Started late " << delta / 1000 << "ms";

    PerIoStruct* aux_local = per_io_.get();
    if (aux_local) {
      record_read.fetch_add(aux_local->records_read, memory_order_relaxed);

      if (aux_local->raw_context) {
        parse_errors.fetch_add(aux_local->raw_context->parse_errors(), memory_order_relaxed);
      }
      std::lock_guard<fibers::mutex> lk(mu);
      input_read.merge(aux_local->inputs_count_map);
    }
  });

  res.emplace_back("parse-errors", VarzValue::FromInt(parse_errors.load()));
  res.emplace_back("records-read", VarzValue::FromInt(record_read.load()));
  res.emplace_back("stats-latency", VarzValue::FromInt(base::GetMonotonicMicrosFast() - start));
  for (const auto& k_v : input_read) {
    res.emplace_back(absl::StrCat("map-input-", k_v.first), VarzValue::FromInt(k_v.second));
  }
  return res;
}

}  // namespace mr3
