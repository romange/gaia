// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
// Example MR that processes movies dataset dataset:
// https://www.kaggle.com/rounakbanik/the-movies-dataset/

#include <rapidjson/error/en.h>
#include <regex>

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

DEFINE_string(dest_dir, "~/mr_output", "");
DEFINE_string(movie_dir, "", "The movies database directory that contains all the files");

static void CleanBadJson(char* b, char* e) {
  // Clean single quotes for keys and optionally for value strings.
  regex re("(')[^']+('): (?:(').*?(')(?:,|}))?");
  cmatch m;

  char* next = b;
  while (regex_search(next, m, re)) {
    VLOG(1) << "Start search " << m.size();
    for (size_t i = 1; i < m.size(); ++i) {
      auto& x = m[i];
      if (x.matched && x.length() == 1) {
        CHECK_EQ('\'', *x.first);

        VLOG(1) << "Found " << x.str();

        char* val_ptr = next + (x.first - next);
        *val_ptr++ = '"';
        if (i == 3) {
          CHECK(m[4].matched);
          char* val_end = next + (m[4].second - next);
          for (char* p = val_ptr; p != val_end; ++p) {
            if (*p == '\\' && p[1] == '\'') {
                *p = ' ';
            } else if (*p == '"') {
              *p = '\'';
            }
          }
        }
      }
    }

    next += (m.position() + m.length());
  }

  // Remove hex escaping
  re.assign(R"(": "([^"]*)\")");
  next = b;
  while (regex_search(next, m, re)) {
    CHECK_EQ(2, m.size());
    auto& x = m[1];
    char* val_ptr = next + (x.first - next);
    char* val_end = next + (x.second - next);
    for (char* p = val_ptr; p != val_end; ++p) {
      if (*p == '\\' && p[1] == 'x' && val_end - p > 3) {
        memset(p, ' ', 4);
      }
    }
    next += (m.position() + m.length());
  }

  constexpr char kBadKey[] = R"("profile_path": None)";
  constexpr size_t kBadLen = sizeof(kBadKey) - 1;

  next = b;
  while (true) {
    next = strstr(next, kBadKey);
    if (!next)
      break;
    memset(next, ' ', kBadLen);
    next += kBadLen;
  }
}

class CreditsMapper {
  std::vector<char*> cols_;

 public:
  void Do(string val, mr3::DoContext<rj::Document>* context);
};

void CreditsMapper::Do(string val, mr3::DoContext<rj::Document>* context) {
  cols_.clear();

  SplitCSVLineWithDelimiter(&val.front(), ',', &cols_);
  CHECK_EQ(3, cols_.size());
  uint32_t movie_id;

  CHECK(absl::SimpleAtoi(cols_[2], &movie_id));

  CleanBadJson(cols_[0], cols_[1]);
  rj::Document doc;
  bool has_error = doc.Parse<rj::kParseTrailingCommasFlag>(cols_[0]).HasParseError();
  CHECK(!has_error) << rj::GetParseError_En(doc.GetParseError()) << cols_[0];
  // cast.AddMember("movie_id", rj::Value(movie_id), cast.GetAllocator());
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
