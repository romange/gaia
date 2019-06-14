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

class JsonCleaner {
 public:
  JsonCleaner();

  void Clean(char* str);

 private:
  // RE2 does not have default c'tor.
  absl::optional<RE2> single_q_re_, str_val_re_;
};

class CreditsMapper {
  std::vector<char*> cols_;

 public:
  void Do(string val, mr3::DoContext<rj::Document>* context);

 private:
  JsonCleaner cleaner_;
};

struct Movie {
  unsigned id;
  string title;
  bool is_adult;
};

namespace mr3 {
template <> class RecordTraits<Movie> {
  std::vector<char*> cols_;

 public:
  static std::string Serialize(bool is_binary, const Movie& rec) {
    return absl::StrCat(rec.id, ", \"", rec.title, "\",", int(rec.is_adult));
  }

  bool Parse(bool is_binary, std::string&& tmp, Movie* res) {
    cols_.clear();

    SplitCSVLineWithDelimiter(&tmp.front(), ',', &cols_);
    CHECK_EQ(3, cols_.size());

    CHECK(absl::SimpleAtoi(cols_[0], &res->id));
    res->is_adult = (*cols_[2] == '1');
    res->title = string(cols_[1]);

    return true;
  }
};
}  // namespace mr3

class MoviesMapper {
  std::vector<char*> cols_;
  string buf_;

 public:
  void Do(string val, mr3::DoContext<Movie>* context);

 private:
  JsonCleaner cleaner_;
};

JsonCleaner::JsonCleaner() {
  // Find all ('key' : value) pairs.
  single_q_re_.emplace("(')[^']+('): (?:(').*?(')(?:,|}))?");

  // Find all (: "str_value") combinations.
  str_val_re_.emplace(R"(": "([^"]+)\")");
}

// std::regex uses recursion with depth of the input string!
// It's awful and can not be used in prod code. We use old n' robust RE2 from Google.
void JsonCleaner::Clean(char* b) {
  // Clean single quotes for for value strings.
  // Clean single quotes from the keys.
  re2::StringPiece input(b);
  re2::StringPiece q[4];
  char *val_b, *val_e;
  while (RE2::FindAndConsume(&input, *single_q_re_, q, q + 1, q + 2, q + 3)) {
    for (unsigned j = 0; j < 4; ++j) {
      if (q[j].size() == 1)
        *const_cast<char*>(q[j].data()) = '"';
    }

    // In case of string values, change their contents to be double quote friendly.
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

void CreditsMapper::Do(string val, mr3::DoContext<rj::Document>* context) {
  cols_.clear();

  SplitCSVLineWithDelimiter(&val.front(), ',', &cols_);
  CHECK_EQ(3, cols_.size());
  uint32_t movie_id;

  CHECK(absl::SimpleAtoi(cols_[2], &movie_id));

  rj::Document cast;
  for (unsigned j = 0; j < 2; ++j) {
    cleaner_.Clean(cols_[j]);
    bool has_error = cast.Parse<rj::kParseTrailingCommasFlag>(cols_[j]).HasParseError();
    CHECK(!has_error) << rj::GetParseError_En(cast.GetParseError()) << cols_[j];
    for (auto& value : cast.GetArray()) {
      rj::Document d;
      d.CopyFrom(value, d.GetAllocator());
      d.AddMember("movie_id", rj::Value(movie_id), d.GetAllocator());

      context->Write(std::move(d));
    }
  }
}

void MoviesMapper::Do(string val, mr3::DoContext<Movie>* context) {
  cols_.clear();
  buf_.append(val);

  string tmp = buf_;
  // adult,belongs_to_collection,budget,genres,homepage,id,imdb_id,original_language,
  // original_title,overview,popularity,poster_path,production_companies,production_countries,
  // release_date,revenue,runtime,spoken_languages,status,tagline,title,video,
  // vote_average,vote_count
  SplitCSVLineWithDelimiter(&tmp.front(), ',', &cols_);
  if (cols_.size() < 24) {
    return;
  }
  CHECK_EQ(24, cols_.size()) << val;

  buf_.clear();
  context->raw()->Inc(absl::StrCat("Adult:", cols_[0]));

  Movie m;
  m.is_adult = (*cols_[0] == 'T');
  CHECK(absl::SimpleAtoi(cols_[5], &m.id));
  m.title = string(cols_[8]);
  context->Write(std::move(m));
}

/* Enriches cast metadata with movie metadata.
   Outputs the enriched metadata of the cast.
   Assumes that movie shard is loaded first, into joiner. The order is controled via
   Pipeline::Join operator
*/
class CastJoiner {
  absl::flat_hash_map<unsigned, Movie> movie_shard_;

 public:
  void OnMovie(Movie m, DoContext<rj::Document>* context);
  void OnCast(rj::Document cast, DoContext<rj::Document>* context);
  void OnShardFinish(DoContext<rj::Document>* cntx);
};

void CastJoiner::OnMovie(Movie m, DoContext<rj::Document>* context) { movie_shard_[m.id] = m; }

void CastJoiner::OnCast(rj::Document cast, DoContext<rj::Document>* context) {
  unsigned id = cast["movie_id"].GetUint();
  auto it = movie_shard_.find(id);
  if (it == movie_shard_.end()) {
    context->raw()->Inc("Movie-NotFound");
    return;
  }
  rj::Value title(it->second.title.c_str(), cast.GetAllocator());

  cast.AddMember("title", title, cast.GetAllocator());
  cast.AddMember("is_adult", rj::Value(it->second.is_adult), cast.GetAllocator());
  context->Write(std::move(cast));
}

void CastJoiner::OnShardFinish(DoContext<rj::Document>* cntx) {
  movie_shard_.clear();
}

class IdentityMapper {
  public:
   void Do(rj::Document doc, mr3::DoContext<rj::Document>* context) {
     context->Write(std::move(doc));
   }
};

inline string MoviePath(const string& filename) {
  return file_util::JoinPath(file_util::ExpandPath(FLAGS_movie_dir), filename);
}

int main(int argc, char** argv) {
  PipelineMain pm(&argc, &argv);

  CHECK(!FLAGS_movie_dir.empty());

  Pipeline* pipeline = pm.pipeline();
  string credit_file = MoviePath("credits*.csv");
  string movies_file = MoviePath("movies_metadata.csv");
  StringTable rc = pipeline->ReadText("read_credits", credit_file).set_skip_header(1);
  StringTable rm = pipeline->ReadText("read_movies", movies_file).set_skip_header(1);

  PTable<rj::Document> cast = rc.Map<CreditsMapper>("MapCrewCast");
  PTable<Movie> movies = rm.Map<MoviesMapper>("MapMovies");

  cast.Write("write_cast", pb::WireFormat::TXT)
      .WithModNSharding(10, [](const rj::Document& r) { return r["movie_id"].GetUint(); })
      .AndCompress(pb::Output::ZSTD);

  movies.Write("write_movies", pb::WireFormat::TXT)
      .WithModNSharding(10, [](const Movie& m) { return m.id; })
      .AndCompress(pb::Output::ZSTD);

  PTable<rj::Document> cast_movies = pipeline->Join<CastJoiner>(
      "Join", {movies.BindWith(&CastJoiner::OnMovie), cast.BindWith(&CastJoiner::OnCast)});
  cast_movies.Write("joined_cast", pb::WireFormat::TXT).AndCompress(pb::Output::ZSTD);

  cast_movies.Map<IdentityMapper>("reshard_cast").Write("adult", pb::WireFormat::TXT)
    .AndCompress(pb::Output::ZSTD).WithCustomSharding([](const rj::Document& doc) {
      bool is_adult = doc["is_adult"].GetBool();
      return is_adult ? "adult" : "family";
    });

  LocalRunner* runner = pm.StartLocalRunner(FLAGS_dest_dir);
  pipeline->Run(runner);

  LOG(INFO) << "After pipeline run";

  return 0;
}
