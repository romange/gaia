// Copyright 2016, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "util/pprint/pprint_utils.h"

#include <iostream>
#include <unordered_map>
#include <glog/stl_logging.h>
#include <google/protobuf/compiler/importer.h>
#include <google/protobuf/dynamic_message.h>

#include "base/flags.h"
#include "base/logging.h"
#include "strings/escaping.h"
#include "strings/numbers.h"
#include "strings/split.h"
#include "strings/strcat.h"

DEFINE_bool(short, false, "");
DEFINE_string(csv, "", "comma delimited list of tag numbers. For repeated fields, it's possible "
                       "to add :[delimiting char] after a tag number.");
DEFINE_bool(use_csv_null, true, "When printing csv format use \\N for outputing undefined "
                                "optional fields.");
DEFINE_bool(aggregate_repeated, false, "When printing csv format, aggregate repeated leaves in one "
                                       "line: \"xx,yy,..\"");

DEFINE_bool(omit_blobs, true, "");
DEFINE_bool(omit_custom_formatting, false, "");
DEFINE_bool(skip_value_escaping, false, "");
DEFINE_string(root_node, "", "");
DEFINE_bool(omit_double_quotes, false, "Omits double quotes when printing string values");

using std::cout;
using std::string;
using std::vector;

using absl::StrCat;

namespace util {
namespace pprint {

FdPath::FdPath(const gpb::Descriptor* root, StringPiece path) {
  std::vector<StringPiece> parts = absl::StrSplit(path, ".");
  CHECK(!parts.empty()) << path;
  const gpb::Descriptor* cur_descr = root;
  for (size_t j = 0; j < parts.size(); ++j) {
    const gpb::FieldDescriptor* field = nullptr;
    uint32 tag_id;

    if (safe_strtou32(parts[j], &tag_id)) {
      field = cur_descr->FindFieldByNumber(tag_id);
    } else {
      string tmp(parts[j].data(), parts[j].size());
      field = cur_descr->FindFieldByName(tmp);
    }

    CHECK(field) << "Can not find tag id " << parts[j];
    if (j + 1 < parts.size()) {
      CHECK_EQ(field->cpp_type(), gpb::FieldDescriptor::CPPTYPE_MESSAGE);
      cur_descr = field->message_type();
    }
    path_.push_back(field);
  }
}

bool FdPath::IsRepeated() const {
  for (auto v : path_) {
    if (v->is_repeated())
      return true;
  }
  return false;
}


void FdPath::ExtractValueRecur(const gpb::Message& msg, uint32 index, ValueCb cb) const {
  CHECK_LT(index, path_.size());
  auto fd = path_[index];
  const gpb::Reflection* reflection = msg.GetReflection();
  uint32 cur_repeated_depth = 0;
  for (uint32 i = 0; i < index; ++i) {
    if (path_[i]->is_repeated()) ++cur_repeated_depth;
  }
  if (fd->is_repeated()) {
    int sz = reflection->FieldSize(msg, fd);
    if (sz > 0) {
      if (index + 1 < path_.size()) {
        // Non leaves, repeated messages.
        if (cur_repeated_depth < cur_repeated_stack_.size()) {
          const gpb::Message& new_msg =
              reflection->GetRepeatedMessage(msg, fd, cur_repeated_stack_[cur_repeated_depth]);
          ExtractValueRecur(new_msg, index + 1, cb);
        } else {
          for (int i = 0; i < sz; ++i) {
            cur_repeated_stack_.push_back(i);
            const gpb::Message& new_msg = reflection->GetRepeatedMessage(msg, fd, i);
            ExtractValueRecur(new_msg, index + 1, cb);
            cur_repeated_stack_.pop_back();
          }
        }

      } else {
        // Repeated leaves.
        if (FLAGS_aggregate_repeated) {
          cb(msg, fd, -1, sz);
        } else {
          for (int i = 0; i < sz; ++i) {
            cb(msg, fd, i, -1);
          }
        }
      }
    }
    return;
  }

  if (index + 1 < path_.size()) {
    const gpb::Message& new_msg = reflection->GetMessage(msg, fd);
    ExtractValueRecur(new_msg, index + 1, cb);
    return;
  }
  /*if (FLAGS_use_csv_null && !reflection->HasField(msg, fd)) {
    cb("\\N");
    return;
  }
  string res;
  printer_.PrintFieldValueToString(msg, fd, -1, &res);*/
  cb(msg, fd, -1, -1);
}

static gpb::SimpleDescriptorDatabase proto_db;
static gpb::DescriptorPool proto_db_pool(&proto_db);

gpb::Message* AllocateMsgByMeta(const string& type, const string& fd_set) {
  CHECK(!type.empty());
  CHECK(!fd_set.empty());


  const gpb::Descriptor* descriptor = proto_db_pool.FindMessageTypeByName(type);
  if (!descriptor) {
    gpb::FileDescriptorSet fd_set_proto;
    CHECK(fd_set_proto.ParseFromString(fd_set));
    for (int i = 0; i < fd_set_proto.file_size(); ++i) {
      // LOG(INFO) << fd_set_proto.file(i).DebugString();
       /*const gpb::FileDescriptor* filed =
          gpb::DescriptorPool::generated_pool()->FindFileByName(fd_set_proto.file(i).name());
       if (filed != nullptr) {
          LOG(INFO) << "Already exists " << filed->name();
       } else {*/
       CHECK(proto_db.Add(fd_set_proto.file(i)));
        // filed = proto_db_pool.BuildFile(fd_set_proto.file(i));
        // VLOG(1) << "Built " << filed->name() << "\n" << filed->DebugString();

      //
      /*vector<int> exts;
      proto_db.FindAllExtensionNumbers("google.protobuf.FieldOptions", &exts);
      LOG(INFO) << "extensions " << exts;*/
    }
    descriptor = proto_db_pool.FindMessageTypeByName(type);
  }

  CHECK(descriptor) << "Can not find " << type << " in the proto pool.";
  return AllocateMsgFromDescr(descriptor);
}

gpb::Message* AllocateMsgFromDescr(const gpb::Descriptor* descr) {
  static gpb::DynamicMessageFactory message_factory(&proto_db_pool);
  message_factory.SetDelegateToGeneratedFactory(true);

  const gpb::Message* msg_proto = message_factory.GetPrototype(descr);
  CHECK_NOTNULL(msg_proto);
  return msg_proto->New();
}

PathNode* PathNode::AddChild(const gpb::FieldDescriptor* fd) {
  for (PathNode& n : children) {
    if (n.fd == fd) return &n;
  }
  children.push_back(PathNode(fd));
  return &children.back();
}

class BetterPrinter : public gpb::TextFormat::FieldValuePrinter {
public:
  virtual string PrintString(const string& val) const override {
    if (FLAGS_omit_blobs) {
      if (val.size() > 100 && std::any_of(val.begin(), val.end(),
                                  [](char c) { return c < 32; })) {
        return "\"Not work safe!\"";
      }
    }
    const string& val2 = FLAGS_skip_value_escaping ? val : absl::Utf8SafeCEscape(val);
    if (FLAGS_omit_double_quotes) {
      return val2;
    }
    return absl::StrCat("\"", val2, "\"");
  }
};

void RegisterCustomFieldPrinter(
      const gpb::Descriptor* descriptor,
      const std::unordered_map<int, const gpb::FieldDescriptor*>& fo_tags_map,
      gpb::TextFormat::Printer* printer) {
  CHECK_NOTNULL(descriptor);

  for (int i = 0; i < descriptor->field_count(); ++i) {
    const gpb::FieldDescriptor* fd = descriptor->field(i);

    if (fd->cpp_type() == gpb::FieldDescriptor::CPPTYPE_MESSAGE) {
      RegisterCustomFieldPrinter(fd->message_type(), fo_tags_map, printer);
      continue;
    }
  }
}

Printer::Printer(const gpb::Descriptor* descriptor): type_name_(descriptor->full_name()) {
  printer_.SetDefaultFieldValuePrinter(new BetterPrinter());
  printer_.SetUseShortRepeatedPrimitives(true);
  // printer_.SetUseUtf8StringEscaping(true);


  std::vector<StringPiece> tags = absl::StrSplit(FLAGS_csv, ",", absl::SkipWhitespace());
  if (tags.empty()) {
    printer_.SetInitialIndentLevel(1);
    printer_.SetSingleLineMode(FLAGS_short);
    if (!FLAGS_root_node.empty()) {
      root_path_ = FdPath{descriptor, FLAGS_root_node};
      CHECK(root_path_.valid());
      const gpb::FieldDescriptor* fd = root_path_.path().back();
      CHECK_EQ(gpb::FieldDescriptor::CPPTYPE_MESSAGE, fd->cpp_type());
    }
  } else {
    for (StringPiece tag_path : tags) {
      FdPath fd_path(descriptor, tag_path);
      PathNode* cur_node = &root_;
      for (const gpb::FieldDescriptor* fd: fd_path.path()) {
        cur_node = cur_node->AddChild(fd);
      }
      fds_.push_back(std::move(fd_path));
    }
  }

  const gpb::Descriptor* fo_descr_root =
      proto_db_pool.FindMessageTypeByName("google.protobuf.FieldOptions");
  if (fo_descr_root == nullptr) {
    fo_descr_root = gpb::DescriptorPool::generated_pool()
           ->FindMessageTypeByName("google.protobuf.FieldOptions");
  }

  CHECK_NOTNULL(fo_descr_root);

  std::unordered_map<int, const gpb::FieldDescriptor*> fo_tags_map;
  vector<const gpb::FieldDescriptor*> fields;
  proto_db_pool.FindAllExtensions(fo_descr_root, &fields);

  for (const gpb::FieldDescriptor* fl : fields) {
    fo_tags_map[fl->number()] = fl;
  }


  RegisterCustomFieldPrinter(descriptor, fo_tags_map, &printer_);

  google::FlushLogFiles(google::GLOG_INFO);

}

void Printer::Output(const gpb::Message& msg) const {
  string text_output;
  if (fds_.empty()) {
    CHECK(printer_.PrintToString(msg, &text_output));
    std::cout << type_name_ << " {" << (FLAGS_short ? " " : "\n")
              << text_output << "}\n";
  } else {
    PrintValueRecur(0, "", false, msg);
  }
}

void Printer::PrintValueRecur(size_t path_index, const string& prefix,
                              bool has_value, const gpb::Message& msg) const {
  CHECK_LT(path_index, fds_.size());
  auto cb_fun = [path_index, this, has_value, &prefix, &msg](
    // num_items - #items in leaf repeated field. if given (!-1): aggregate all values: "xx,yy,.."
    // item_index - item index in leaf repeated field. if given (!-1): print line with this item.
    const gpb::Message& parent, const gpb::FieldDescriptor* fd, int item_index, int num_items) {
    string val;
    CHECK_NE(num_items, 0);

    if (num_items != -1) {
      if (!FLAGS_omit_double_quotes)
        val = "\"";
      for (int i=0; i < num_items; i++) {
        string repeated_val;
        printer_.PrintFieldValueToString(parent, fd, i, &repeated_val);
        absl::StrAppend(&val, repeated_val, ",");
      }
      if (FLAGS_omit_double_quotes)
        val.pop_back();
      else
        val.back() = '"';
    } else {
      printer_.PrintFieldValueToString(parent, fd, item_index, &val);
      if (item_index == -1) {
        const gpb::Reflection* reflection = parent.GetReflection();
        if (FLAGS_use_csv_null && !reflection->HasField(parent, fd)) {
          val = "\\N";
        }
      }
    }

    string next_val = (path_index == 0) ? val : StrCat(prefix, ",", val);
    bool next_has_value = has_value | !val.empty();
    if (path_index + 1 == fds_.size()) {
      if (next_has_value)
        cout << next_val << std::endl;
    } else {
      PrintValueRecur(path_index + 1, next_val, next_has_value, msg);
    }
  };
  fds_[path_index].ExtractValue(msg, cb_fun);
}

using FD = gpb::FieldDescriptor;

static void PrintBqSchemaInternal(unsigned offset, const gpb::Descriptor* descr) {
  cout << "[\n";
  bool continuation_field = false;
  for (int i = 0; i < descr->field_count(); ++i) {
    const gpb::FieldDescriptor* fd = descr->field(i);
    string fname = fd->name();
    CHECK(!fname.empty());

    if (continuation_field) {
      cout << ",\n";  // Finalize previous field.
    }

    continuation_field = true;
    cout << string(offset, ' ') << R"(  { "name": ")" << fname << R"(",  "type": ")";

    switch (fd->cpp_type()) {
      case FD::CPPTYPE_INT32:
      case FD::CPPTYPE_UINT32:
      case FD::CPPTYPE_INT64:
      case FD::CPPTYPE_UINT64:
        cout << "INTEGER\"";
      break;
      case FD::CPPTYPE_BOOL:
        cout << "BOOLEAN\"";
      break;

      case FD::CPPTYPE_STRING:
        cout << "STRING\"";
      break;
      case FD::CPPTYPE_DOUBLE:
      case FD::CPPTYPE_FLOAT:
        cout << "FLOAT\"";
      break;
      case FD::CPPTYPE_ENUM:
        cout << "INTEGER\"";
      break;
      case FD::CPPTYPE_MESSAGE:
        cout << R"(RECORD",  "fields": )";
        PrintBqSchemaInternal(offset + 2, fd->message_type());
        cout << string(offset + 4, ' ');
      break;
      default:
        LOG(FATAL) << " not supported " << fd->cpp_type_name();
    }
    if (fd->is_repeated()) {
      cout << R"(, "mode": "REPEATED")";
    } else if (fd->is_required()) {
      cout << R"(, "mode": "REQUIRED")";
    }
    cout << " }";
  }
  cout << " ]\n";
}

void PrintBqSchema(const gpb::Descriptor* descr) {
  PrintBqSchemaInternal(0, descr);
}

static std::vector<const gpb::FieldDescriptor *> ListFields(const gpb::Message &msg) {
  std::vector<const gpb::FieldDescriptor *> initialized_fields;
  msg.GetReflection()->ListFields(msg, &initialized_fields);
  return initialized_fields;
}

static size_t GetSize(const gpb::Message &msg,
                      const gpb::FieldDescriptor *field) {
  const gpb::Reflection *reflect = msg.GetReflection();
  const size_t field_size = field->is_repeated() ? reflect->FieldSize(msg, field) : 1;
  // TODO(ORI): Need to handle variant encoding in protobufs
  // (otherwise our calculation of the integral fields is very inaccurate)
  switch (field->type()) {
    case gpb::FieldDescriptor::TYPE_DOUBLE:
    case gpb::FieldDescriptor::TYPE_INT64:
    case gpb::FieldDescriptor::TYPE_UINT64:
    case gpb::FieldDescriptor::TYPE_FIXED64:
    case gpb::FieldDescriptor::TYPE_SFIXED64:
    case gpb::FieldDescriptor::TYPE_SINT64:
      return 8 * field_size;
    case gpb::FieldDescriptor::TYPE_FLOAT:
    case gpb::FieldDescriptor::TYPE_INT32:
    case gpb::FieldDescriptor::TYPE_UINT32:
    case gpb::FieldDescriptor::TYPE_FIXED32:
    case gpb::FieldDescriptor::TYPE_SFIXED32:
    case gpb::FieldDescriptor::TYPE_SINT32:
    case gpb::FieldDescriptor::TYPE_ENUM: // TODO(ORI): Is this correct?
      return 4 * field_size;
    case gpb::FieldDescriptor::TYPE_BOOL:
      return field_size;
    case gpb::FieldDescriptor::TYPE_STRING:
    case gpb::FieldDescriptor::TYPE_BYTES: {
      std::string temp;
      if (field->is_repeated()) {
        size_t sum = 0;
        for (size_t i = 0; i < field_size; ++i)
          sum += reflect->GetRepeatedStringReference(msg, field, i, &temp).size();
        return sum;
      } else {
        return reflect->GetStringReference(msg, field, &temp).size();
      }
    }
    default:
      LOG(FATAL) << " not supported " << field->type();
      return -1;
  }
}

static SizeSummarizer::Trie FillTrie(const gpb::Descriptor *descr) {
  using Trie = SizeSummarizer::Trie;
  Trie trie;
  trie.Resize(descr->field_count());
  for (int i = 0; i < descr->field_count(); ++i) {
    if (descr->field(i)->type() == gpb::FieldDescriptor::TYPE_MESSAGE)
      trie.Put(i, std::unique_ptr<Trie>(new Trie(FillTrie(descr->field(i)->message_type()))));
    else
      trie.Put(i, std::unique_ptr<Trie>(new Trie));
    trie.Get(i)->name = descr->field(i)->name();
  }
  return trie;
}

SizeSummarizer::SizeSummarizer(const gpb::Descriptor *descr)
    : trie_(FillTrie(descr)) {}

static size_t AddSizesImpl(const gpb::Message &msg,
                         SizeSummarizer::Trie *trie) {
  size_t ret = 0;
  for (const auto &field : ListFields(msg)) {
    size_t sz;
    auto subtrie = trie->Get(field->index());
    if (field->type() == gpb::FieldDescriptor::TYPE_MESSAGE) {
      const gpb::Reflection *reflect = msg.GetReflection();
      if (field->is_repeated()) {
        size_t field_size = reflect->FieldSize(msg, field);
        sz = 0;
        for (size_t i = 0; i < field_size; ++i) {
          const gpb::Message &msg2 = reflect->GetRepeatedMessage(msg, field, i);
          sz += AddSizesImpl(msg2, subtrie);
        }
      } else {
        const gpb::Message &msg2 = reflect->GetMessage(msg, field);
        sz = AddSizesImpl(msg2, subtrie);
      }
    } else {
      sz = GetSize(msg, field);
    }
    subtrie->bytes += sz;
    ret += sz;
  }
  return ret;
}

void SizeSummarizer::AddSizes(const gpb::Message &msg) {
  AddSizesImpl(msg, &trie_);
}

static void GetSizesImpl(const SizeSummarizer::Trie &trie,
                         const std::string &path,
                         std::map<std::string, size_t> *out) {
  std::string new_path;
  if (path.empty()) {
    if (trie.name.empty())
      new_path = path;
    else
      new_path = trie.name;
  } else {
    CHECK(!trie.name.empty());
    new_path = path + "." + trie.name;
  }

  if (trie.bytes) {
    auto iter_and_is_new = out->emplace(new_path, trie.bytes);
    auto iter = iter_and_is_new.first;
    bool is_new = iter_and_is_new.second;
    CHECK(is_new);
    iter->second = trie.bytes;
  }

  for (size_t i = 0; i < trie.Size(); ++i)
    GetSizesImpl(*trie.Get(i), new_path, out);

}

std::map<std::string, size_t> SizeSummarizer::GetSizes() const {
  std::map<std::string, size_t> ret;
  GetSizesImpl(trie_, "", &ret);
  return ret;
}

void SizeSummarizer::Print(std::ostream *out_p) const {
  for (const auto &name_and_size : this->GetSizes())
    std::cout << name_and_size.first << " - " << name_and_size.second << "\n";
}
}  // namespace pprint
}  // namespace util
