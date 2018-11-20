// Copyright 2013, .com .  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include <signal.h>
#include <sys/stat.h>

#include <google/protobuf/compiler/importer.h>
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/text_format.h>

#include "absl/strings/match.h"

#include "base/init.h"
#include "base/map-util.h"
#include "file/list_file.h"
#include "file/proto_writer.h"
#include "strings/escaping.h"
#include "util/pprint/file_printer.h"
#include "util/pprint/pprint_utils.h"

DEFINE_string(protofiles, "", "");
DEFINE_string(proto_db_file, "s3://test/roman/proto_db.lst", "");
DEFINE_string(type, "", "");
DECLARE_string(csv);

DECLARE_bool(sizes);
DECLARE_bool(raw);
DECLARE_bool(count);

using namespace util::pprint;
namespace gpc = gpb::compiler;
using std::string;
using strings::AsString;
using util::Status;

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

void sigpipe_handler(int signal) {
  exit(1);
}

class ListReaderPrinter final : public FilePrinter {
 protected:
  void LoadFile(const std::string& fname) override;
  util::StatusObject<bool> Next(StringPiece* record) override;
  void PostRun() override;

 private:
  std::unique_ptr<file::ListReader> reader_;
  string record_buf_;
  util::Status st_;
};

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

int main(int argc, char** argv) {
  MainInitGuard guard(&argc, &argv);

  signal(SIGPIPE, sigpipe_handler);

  size_t count = 0;

  // const Reflection* reflection = msg->GetReflection();
  for (int i = 1; i < argc; ++i) {
    StringPiece path(argv[i]);
    LOG(INFO) << "Opening " << path;

    if (absl::EndsWith(path, ".sst")) {
      LOG(FATAL) << "Not supported " << path;
    } else {
      ListReaderPrinter printer;
      printer.Init(argv[i]);
      auto st = printer.Run();
      CHECK_STATUS(st);
      count += printer.count();
    }
  }
  if (FLAGS_count)
    std::cout << "Count: " << count << std::endl;

  return 0;
}
