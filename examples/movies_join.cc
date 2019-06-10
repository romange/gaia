// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
// Example MR that processes movies dataset dataset:
// https://www.kaggle.com/rounakbanik/the-movies-dataset/

#include <rapidjson/error/en.h>

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

/*struct CastCrewObj {
  rj::Document cast, crew;
  uint32_t movie_id;
};

namespace mr3 {

template <> class RecordTraits<CastCrewObj> {
  std::vector<char*> cols_;

 public:
  static std::string Serialize(bool is_binary, const CastCrewObj& rec) {
    return absl::StrCat(rec.movie_id);
  }

  bool Parse(bool is_binary, std::string&& tmp, CastCrewObj* res) {

    return true;
  }
};
}  // namespace mr3
*/


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
  rj::Document cast;
  cast.ParseInsitu<rj::kParseDefaultFlags>(cols_[0]);

  bool has_error = cast.HasParseError();
  LOG_IF(FATAL, has_error) << rj::GetParseError_En(cast.GetParseError()) << " for string "
                           << cols_[0];
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
