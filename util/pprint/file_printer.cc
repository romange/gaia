// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (roman@ubimo.com)
//
#include "util/pprint/file_printer.h"

#include <google/protobuf/compiler/importer.h>

#include "absl/strings/escaping.h"
#include "base/flags.h"
#include "base/hash.h"
#include "base/logging.h"
#include "base/map-util.h"
#include "file/list_file.h"
#include "file/proto_writer.h"
#include "util/pb2json.h"
#include "util/plang/plang_parser.hh"
#include "util/plang/plang_scanner.h"
#include "util/pprint/pprint_utils.h"

DEFINE_string(protofiles, "", "");
DEFINE_string(proto_db_file, "s3://test/roman/proto_db.lst", "");
DEFINE_string(type, "", "");

DEFINE_string(where, "", "boolean constraint in plang language");
DEFINE_bool(sizes, false, "Prints a rough estimation of the size of every field");
DEFINE_bool(json, false, "");
DEFINE_bool(raw, false, "");
DEFINE_string(sample_key, "", "");
DEFINE_int32(sample_factor, 0, "If bigger than 0 samples and outputs record once in k times");
DEFINE_bool(parallel, true, "");
DEFINE_bool(count, false, "");


namespace util {
namespace pprint {

using namespace std;
using namespace file;
namespace gpc = gpb::compiler;

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

static bool ShouldSkip(const gpb::Message& msg, const FdPath& fd_path) {
  if (FLAGS_sample_factor <= 0 || FLAGS_sample_key.empty())
    return false;
  const string* val = nullptr;
  string buf;
  auto cb = [&val, &buf](const gpb::Message& msg, const gpb::FieldDescriptor* fd, int, int) {
    const gpb::Reflection* refl = msg.GetReflection();
    val = &refl->GetStringReference(msg, fd, &buf);
  };
  fd_path.ExtractValue(msg, cb);
  CHECK(val);

  uint32 num = base::Fingerprint(*val);

  return (num % FLAGS_sample_factor) != 0;
}

class FilePrinter::PrintTask {
 public:
  typedef PrintSharedData* SharedData;

  void InitShared(SharedData d) {
    shared_data_ = d;
  }

  PrintTask(const gpb::Message* to_clone, const Pb2JsonOptions& opts) : options_(opts) {
    if (to_clone) {
      local_msg_.reset(to_clone->New());
    }
    if (!FLAGS_sample_key.empty()) {
      fd_path_ = FdPath(to_clone->GetDescriptor(), FLAGS_sample_key);
      CHECK(!fd_path_.IsRepeated());
      CHECK_EQ(gpb::FieldDescriptor::CPPTYPE_STRING, fd_path_.path().back()->cpp_type());
    }
  }

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
      string str = Pb2Json(*local_msg_, options_);
      std::cout << str << "\n";
    } else {
      shared_data_->printer->Output(*local_msg_);
    }
  }

 private:
  std::unique_ptr<gpb::Message> local_msg_;
  FdPath fd_path_;
  SharedData shared_data_;
  Pb2JsonOptions options_;
};

FilePrinter::FilePrinter() {}
FilePrinter::~FilePrinter() {}

void FilePrinter::Init(const string& fname) {
  CHECK(!descr_msg_);

  if (!FLAGS_where.empty()) {
    std::istringstream istr(FLAGS_where);
    plang::Scanner scanner(&istr);
    plang::Parser parser(&scanner, &test_expr_);
    CHECK_EQ(0, parser.parse()) << "Could not parse " << FLAGS_where;
  }

  LoadFile(fname);

  if (descr_msg_) {
    if (FLAGS_sizes)
      size_summarizer_.reset(new SizeSummarizer(descr_msg_->GetDescriptor()));
    printer_.reset(new Printer(descr_msg_->GetDescriptor(), field_printer_cb_));
  } else {
    CHECK(!FLAGS_sizes);
  }

  pool_.reset(new TaskPool("pool", 10));

  shared_data_.size_summarizer = size_summarizer_.get();
  shared_data_.printer = printer_.get();
  shared_data_.expr = test_expr_.get();
  pool_->SetSharedData(&shared_data_);
  pool_->Launch(descr_msg_.get(), options_);

  if (FLAGS_parallel) {
    LOG(INFO) << "Running in parallel " << pool_->thread_count() << " threads";
  }
}

auto FilePrinter::GetDescriptor() const -> const Descriptor* {
  return descr_msg_ ? descr_msg_->GetDescriptor() : nullptr;
}

Status FilePrinter::Run() {
  StringPiece record;
  while (true) {
    // Reads raw record from the file.
    util::StatusObject<bool> res = Next(&record);
    if (!res.ok())
      return res.status;
    if (!res.obj)
      break;
    if (FLAGS_count) {
      ++count_;
    } else {
      if (FLAGS_parallel) {
        pool_->RunTask(AsString(record));
      } else {
        pool_->RunInline(AsString(record));
      }
    }
  }
  pool_->WaitForTasksToComplete();

  if (size_summarizer_.get())
    std::cout << *size_summarizer_ << "\n";
  return Status::OK;
}


void ListReaderPrinter::LoadFile(const std::string& fname) {
  auto corrupt_cb = [this](size_t bytes, const util::Status& status) { st_ = status; };

  reader_.reset(new ListReader(fname, false, corrupt_cb));

  if (!FLAGS_raw && !FLAGS_count) {
    std::map<std::string, std::string> meta;
    if (!reader_->GetMetaData(&meta)) {
      LOG(FATAL) << "Error fetching metadata from " << fname;
    }
    string ptype = FindValueWithDefault(meta, file::kProtoTypeKey, string());
    string fd_set = FindValueWithDefault(meta, file::kProtoSetKey, string());
    if (!ptype.empty() && !fd_set.empty())
      descr_msg_.reset(AllocateMsgByMeta(ptype, fd_set));
    else
      descr_msg_.reset(AllocateMsgFromDescr(FindDescriptor()));
  }
}

util::StatusObject<bool> ListReaderPrinter::Next(StringPiece* record) {
  bool res = reader_->ReadRecord(record, &record_buf_);
  if (!st_.ok())
    return st_;

  return res;
}

void ListReaderPrinter::PostRun() {
  LOG(INFO) << "Data bytes: " << reader_->read_data_bytes()
            << " header bytes: " << reader_->read_header_bytes();
}

}  // namespace pprint
}  // namespace util
