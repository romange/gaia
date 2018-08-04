// Copyright 2013, .com .  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include <signal.h>
#include <sys/stat.h>

#include <google/protobuf/compiler/importer.h>
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/text_format.h>

#include <mutex>
#include "base/init.h"
#include "base/hash.h"

#include "file/list_file.h"
#include "file/proto_writer.h"
#include "absl/strings/match.h"
#include "strings/escaping.h"

#include "util/plang/plang.h"
#include "util/plang/plang_parser.hh"
#include "util/plang/plang_scanner.h"
#include "util/pprint/pprint_utils.h"
#include "util/pb2json.h"

#include "base/map-util.h"
#include "util/sp_task_pool.h"

DEFINE_string(protofiles, "", "");
DEFINE_string(proto_db_file, "s3://test/roman/proto_db.lst", "");
DEFINE_string(type, "", "");
DEFINE_string(where, "", "boolean constraint in plang language");
DECLARE_string(csv);
DEFINE_bool(json, false, "");
DEFINE_bool(raw, false, "");
DEFINE_string(schema, "", "Prints the schema of the underlying proto."
                          "Can be either 'json' or 'proto'.");
DEFINE_bool(sizes, false, "Prints a rough estimation of the size of every field");
DEFINE_bool(parallel, true, "");
DEFINE_string(sample_key, "", "");
DEFINE_int32(sample_factor, 0, "If bigger than 0 samples and outputs record once in k times");

using namespace util::pprint;
namespace gpc = gpb::compiler;
using std::string;
using strings::AsString;

class ErrorCollector : public gpc::MultiFileErrorCollector {
  void AddError(const string& filenname, int line, int column, const string& message) {
    std::cerr << "Error File : " << filenname << " : " << message << std::endl;
  }
};

static const gpb::Descriptor* FindDescriptor() {
  CHECK(!FLAGS_type.empty()) << "type must be filled. For example: --type=foursquare.Category";
  const gpb::DescriptorPool* gen_pool = gpb::DescriptorPool::generated_pool();
  const gpb::Descriptor* descriptor = gen_pool->FindMessageTypeByName(FLAGS_type);
  if (descriptor)
    return descriptor;

  gpc::DiskSourceTree tree;
  tree.MapPath("START_FILE", FLAGS_protofiles);
  ErrorCollector collector;
  gpc::Importer importer(&tree, &collector);
  if (!FLAGS_protofiles.empty()) {
    // TODO: to support multiple files some day.
    CHECK(importer.Import("START_FILE"));
  }
  descriptor = importer.pool()->FindMessageTypeByName(FLAGS_type);
  if (descriptor)
    return descriptor;
  static gpb::SimpleDescriptorDatabase proto_db;
  static gpb::DescriptorPool proto_db_pool(&proto_db);
  file::ListReader reader(FLAGS_proto_db_file);
  string record_buf;
  StringPiece record;
  while (reader.ReadRecord(&record, &record_buf)) {
    gpb::FileDescriptorProto* fdp = new gpb::FileDescriptorProto;
    CHECK(fdp->ParseFromArray(record.data(), record.size()));
    proto_db.AddAndOwn(fdp);
  }
  descriptor = proto_db_pool.FindMessageTypeByName(FLAGS_type);
  CHECK(descriptor) << "Can not find " << FLAGS_type << " in the proto pool.";
  return descriptor;
}

using std::cout;

using namespace file;

using std::mutex;

struct PrintSharedData {
  mutex m;
  const plang::Expr* expr = nullptr;
  const Printer* printer = nullptr;
  SizeSummarizer *size_summarizer = nullptr;
};


bool ShouldSkip(const gpb::Message& msg, const FdPath& fd_path) {
  if (FLAGS_sample_factor <= 0 || FLAGS_sample_key.empty())
    return false;
  const string* val = nullptr;
  string buf;
  auto cb = [&val, &buf](const gpb::Message& msg, const gpb::FieldDescriptor* fd, int) {
    const gpb::Reflection* refl = msg.GetReflection();
    val = &refl->GetStringReference(msg, fd, &buf);
  };
  fd_path.ExtractValue(msg, cb);
  CHECK(val);

  uint32 num = base::Fingerprint(*val);

  return (num % FLAGS_sample_factor) != 0;
}

class PrintTask {
 public:
  typedef PrintSharedData* SharedData;

  void InitShared(SharedData d) { shared_data_ = d;}

  void operator()(const std::string& obj) {
    if (FLAGS_raw) {
      std::lock_guard<mutex> lock(shared_data_->m);
      std::cout << absl::Utf8SafeCEscape(obj) << "\n";
      return;
    }
    CHECK(local_msg_->ParseFromString(obj));
    if (shared_data_->expr && !plang::EvaluateBoolExpr(*shared_data_->expr, *local_msg_))
      return;

    if (ShouldSkip(*local_msg_, fd_path_))
      return;
    std::lock_guard<mutex> lock(shared_data_->m);

    if (FLAGS_sizes) {
      shared_data_->size_summarizer->AddSizes(*local_msg_);
      return;
    }

    if (FLAGS_json) {
      string str = util::Pb2Json(*local_msg_);
      std::cout << str << "\n";
    } else {
      shared_data_->printer->Output(*local_msg_);
    }
  }

  explicit PrintTask(gpb::Message* to_clone) {
    if (to_clone) {
      local_msg_.reset(to_clone->New());
    }
    if (!FLAGS_sample_key.empty()) {
      fd_path_ = FdPath(to_clone->GetDescriptor(), FLAGS_sample_key);
      CHECK(!fd_path_.IsRepeated());
      CHECK_EQ(gpb::FieldDescriptor::CPPTYPE_STRING, fd_path_.path().back()->cpp_type());
    }
  }

 private:
  std::unique_ptr<gpb::Message> local_msg_;
  FdPath fd_path_;
  SharedData shared_data_;
};


void sigpipe_handler(int signal) {
  exit(1);
}

int main(int argc, char **argv) {
  MainInitGuard guard(&argc, &argv);

  signal(SIGPIPE, sigpipe_handler);

  std::unique_ptr<plang::Expr> test_expr;
  if (!FLAGS_where.empty()) {
    std::istringstream istr(FLAGS_where);
    plang::Scanner scanner(&istr);
    plang::Parser parser(&scanner, &test_expr);
    CHECK_EQ(0, parser.parse()) << "Could not parse " << FLAGS_where;
  }

  // const Reflection* reflection = msg->GetReflection();
  for (int i = 1; i < argc; ++i) {
    StringPiece path(argv[i]);
    LOG(INFO) << "Opening " << path;

    string ptype;
    string fd_set;
    const string kEmptyKey;
    std::unique_ptr<gpb::Message> tmp_msg;

    if (absl::EndsWith(path, ".sst")) {
      LOG(FATAL) << "Not supported " << path;
    } else {
      file::ListReader reader(argv[i]);
      string record_buf;
      StringPiece record;
      std::unique_ptr<Printer> printer;
      std::unique_ptr<SizeSummarizer> size_summarizer;

      if (!FLAGS_raw) {
        std::map<std::string, std::string> meta;
        if (!reader.GetMetaData(&meta)) {
          LOG(ERROR) << "Error reading " << argv[i];
          return 1;
        }
        ptype = FindWithDefault(meta, file::kProtoTypeKey, kEmptyKey);
        fd_set = FindWithDefault(meta, file::kProtoSetKey, kEmptyKey);
        if (!ptype.empty() && !fd_set.empty())
          tmp_msg.reset(AllocateMsgByMeta(ptype, fd_set));
        else
          tmp_msg.reset(AllocateMsgFromDescr(FindDescriptor()));

        if (!FLAGS_schema.empty()) {
          if (FLAGS_schema == "json") {
            PrintBqSchema(tmp_msg->GetDescriptor());
          } else if (FLAGS_schema == "proto") {
            cout << tmp_msg->GetDescriptor()->DebugString() << std::endl;
          } else {
            LOG(FATAL) << "Unknown schema";
          }
          return 0;
        }

        if (FLAGS_sizes)
          size_summarizer.reset(new SizeSummarizer(tmp_msg->GetDescriptor()));
        printer.reset(new Printer(tmp_msg->GetDescriptor()));
      }

      using TaskPool = util::SingleProducerTaskPool<PrintTask, string>;
      std::unique_ptr<TaskPool> pool;
      pool.reset(new TaskPool("pool", 10));

      PrintSharedData shared_data;
      shared_data.size_summarizer = size_summarizer.get();
      shared_data.printer = printer.get();
      shared_data.expr = test_expr.get();
      pool->SetSharedData(&shared_data);

      pool->Launch(tmp_msg.get());

      if (FLAGS_parallel) {
        LOG(INFO) << "Running in parallel " << pool->thread_count() << " threads";
      }

      while (reader.ReadRecord(&record, &record_buf)) {
        if (FLAGS_parallel) {
          pool->RunTask(AsString(record));
        } else {
          pool->RunInline(AsString(record));
        }
      }
      if (pool)
        pool->WaitForTasksToComplete();
      if (size_summarizer)
        std::cout << *size_summarizer << "\n";
      LOG(INFO) << "Data bytes: " << reader.read_data_bytes() << " header bytes: "
                << reader.read_header_bytes();
    }
  }

  return 0;
}
