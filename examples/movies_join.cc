// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
// Example MR that processes movies dataset dataset:
// https://www.kaggle.com/rounakbanik/the-movies-dataset/

#include <rapidjson/error/en.h>
#include <re2/re2.h>

#include "base/init.h"
#include "base/logging.h"
#include "file/file_util.h"

#include "mr/local_runner.h"
#include "mr/mr_main.h"

#include "absl/strings/str_cat.h"
#include "strings/split.h"

using namespace mr3;
using namespace util;
using namespace std;
namespace rj = rapidjson;
using re2::RE2;

DEFINE_string(dest_dir, "~/mr_output", "");
DEFINE_string(movie_dir, "", "The movies database directory that contains all the files");

class CreditsMapper {
  std::vector<char*> cols_;

 public:
  CreditsMapper() {
    single_q_re_.emplace("(')[^']+('): (?:(').*?(')(?:,|}))?");
    str_val_re_.emplace(R"(": "([^"]+)\")");
  }

  void Do(string val, mr3::DoContext<rj::Document>* context);

 private:

  void CleanBadJson(char* b, char* e);

  absl::optional<RE2> single_q_re_, str_val_re_;
};

void CreditsMapper::Do(string val, mr3::DoContext<rj::Document>* context) {
  cols_.clear();

  SplitCSVLineWithDelimiter(&val.front(), ',', &cols_);
  CHECK_EQ(3, cols_.size());
  uint32_t movie_id;

  CHECK(absl::SimpleAtoi(cols_[2], &movie_id));

  CleanBadJson(cols_[0], cols_[1]);
  rj::Document cast;
  bool has_error = cast.Parse<rj::kParseTrailingCommasFlag>(cols_[0]).HasParseError();
  CHECK(!has_error) << rj::GetParseError_En(cast.GetParseError()) << cols_[0];
  for (auto& value : cast.GetArray()) {
    // rj::Value v = value.Get
    value.AddMember("movie_id", rj::Value(movie_id), cast.GetAllocator());

    rj::Document d;
    d.CopyFrom(value, d.GetAllocator());
    context->Write(std::move(d));
  }
  // cast.AddMember(
}

// std::regex uses recursion with length of the input string!
// it can not be used in prod code.
void CreditsMapper::CleanBadJson(char* b, char* e) {
  // Clean single quotes for for value strings.
  // Clean single quotes from the keys.
  re2::StringPiece input(b);
  re2::StringPiece q[4];
  char* val_b, *val_e;
  while (RE2::FindAndConsume(&input, *single_q_re_, q, q + 1, q + 2, q + 3)) {
    for (unsigned j = 0; j < 4; ++j) {
      if (q[j].size() == 1)
        *const_cast<char*>(q[j].data()) = '"';
    }
    if (!q[2].empty()) {
      val_b = const_cast<char*>(q[2].data()) + 1;
      val_e = const_cast<char*>(q[3].data());
      for (; val_b != val_e; ++val_b) {
        if (*val_b == '\\' && val_b[1] == '\'') {
          *val_b = ' ';
        } else if (*val_b == '"') {
          *val_b = '\'';
        }
      }
    }
  }

  input.set(b);
  // Remove hex escaping from strings.
  while (RE2::FindAndConsume(&input, *str_val_re_, q)) {
    CHECK(!q[0].empty());
    val_b = const_cast<char*>(q[0].data());
    val_e = val_b + q[0].size();
    for (; val_b != val_e; ++val_b) {
      if (*val_b == '\\' && val_b[1] == 'x' && val_e - val_b > 3) {
        memset(val_b, ' ', 4);
      }
    }
  }

  constexpr char kBadKey[] = R"("profile_path": None)";
  constexpr size_t kBadLen = sizeof(kBadKey) - 1;

  char* next = b;
  while (true) {
    next = strstr(next, kBadKey);
    if (!next)
      break;
    memset(next, ' ', kBadLen);
    next += kBadLen;
  }
}

inline string MoviePath(const string& filename) {
  return file_util::JoinPath(file_util::ExpandPath(FLAGS_movie_dir), filename);
}

int main(int argc, char** argv) {
  PipelineMain pm(&argc, &argv);

  CHECK(!FLAGS_movie_dir.empty());

  Pipeline* pipeline = pm.pipeline();
  string credit_file = MoviePath("credits.csv");
  StringTable ss = pipeline->ReadText("gsod", credit_file).set_skip_header(1);

  PTable<rj::Document> records = ss.Map<CreditsMapper>("MapCrewCast");

  records.Write("write_cast", pb::WireFormat::TXT)
      .WithModNSharding(10, [](const rj::Document& r) { return r["movie_id"].GetUint(); })
      .AndCompress(pb::Output::ZSTD);

  LocalRunner* runner = pm.StartLocalRunner(FLAGS_dest_dir);
  pipeline->Run(runner);

  LOG(INFO) << "After pipeline run";

  return 0;
}
