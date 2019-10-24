// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
// Parses WET files https://commoncrawl.org/the-data/get-started/#WET-Format from
// common crawl dataset. For example, check out file
// s3://commoncrawl/crawl-data/CC-MAIN-2018-22/segments/1526794863277.18/wet/CC-MAIN-20180520092830-20180520112830-00000.warc.wet.gz
#include <hs/hs.h>
#include <re2/re2.h>

#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_split.h"

#include "base/hash.h"
#include "base/init.h"
#include "base/logging.h"

#include "absl/strings/strip.h"
#include "file/file_util.h"

#include "mr/local_runner.h"
#include "mr/mr_main.h"
#include "mr/pipeline.h"
#include "strings/unique_strings.h"

using namespace std;
using namespace boost;
using namespace util;

DEFINE_string(dest_dir, "~/mr_output", "");
DEFINE_uint32(num_shards, 10, "");
DEFINE_int32(compress_level, 1, "");
DEFINE_string(pattern, "foobar", "");

using namespace boost;
using namespace mr3;
using namespace util;
using re2::RE2;

struct WordCount {
  string word;
  uint64_t cnt;
};

namespace mr3 {
template <> class RecordTraits<WordCount> {
 public:
  static std::string Serialize(bool is_binary, const WordCount& rec) {
    CHECK_EQ(string::npos, rec.word.find(':'));

    return absl::StrCat(rec.word, ": ", rec.cnt);
  }

  bool Parse(bool is_binary, std::string&& tmp, WordCount* res) {
    vector<StringPiece> v = absl::StrSplit(tmp, ':');
    if (v.size() != 2)
      return false;

    res->word = string(v[0]);
    return absl::SimpleAtoi(v[1], &res->cnt);
  }
};

}  // namespace mr3

class WordCountTable {
 public:
  WordCountTable() { word_cnts_.set_empty_key(StringPiece()); }
  void AddWord(StringPiece word, uint64_t count) { word_cnts_[word] += count; }

  void Flush(DoContext<WordCount>* cntx) {
    for (const auto& k_v : word_cnts_) {
      cntx->Write(WordCount{string{k_v.first}, k_v.second});
    }
    word_cnts_.clear();
  }

  size_t MemoryUsage() const { return word_cnts_.MemoryUsage(); }
  size_t size() const { return word_cnts_.size(); }

 private:
  StringPieceDenseMap<uint64_t> word_cnts_;
};

class WordSplitter {
 public:
  WordSplitter(const hs_database_t* db);

  ~WordSplitter() { hs_free_scratch(scratch_); }

  void Do(string did, DoContext<WordCount>* cntx);

  void OnShardFinish(DoContext<WordCount>* cntx);

 private:
  static int OnMatch(unsigned int id, unsigned long long from, unsigned long long to,
                     unsigned int flags, void* context);

  absl::optional<RE2> re_;
  WordCountTable word_table_;
  const hs_database_t* hs_db_;
  hs_scratch_t* scratch_ = nullptr;
};

WordSplitter::WordSplitter(const hs_database_t* db) : hs_db_(db) {
  re_.emplace("\\p{L}+");

  hs_error_t err = hs_alloc_scratch(hs_db_, &scratch_);
  CHECK_EQ(HS_SUCCESS, err);
}

void WordSplitter::Do(string line, DoContext<WordCount>* cntx) {
  re2::StringPiece line_re2(line), word;

  while (RE2::FindAndConsume(&line_re2, *re_, &word)) {
    LOG(INFO) << "Adding " << word;

    word_table_.AddWord(absl::string_view{word.begin(), word.size()}, 1);
  }
  hs_error_t err = hs_scan(hs_db_, line.data(), line.size(), 0, scratch_, &OnMatch, this);
  CHECK_EQ(HS_SUCCESS, err);

  if (word_table_.size() > 200000 && word_table_.MemoryUsage() > 256 * 1000000ULL) {
    word_table_.Flush(cntx);
  }
}

void WordSplitter::OnShardFinish(DoContext<WordCount>* cntx) {
  LOG(INFO) << "OnShardFinish";

  word_table_.Flush(cntx);
}

int WordSplitter::OnMatch(unsigned int id, unsigned long long from, unsigned long long to,
                          unsigned int flags, void* context) {
  printf("from %lld, to %lld \n", from, to);
  return 0;  // Continue
}

// Joiner code
class WordGroupBy {
 public:
  void OnWordCount(WordCount wc, DoContext<WordCount>* context) {
    word_table_.AddWord(wc.word, wc.cnt);
  }

  void OnShardFinish(DoContext<WordCount>* cntx) { word_table_.Flush(cntx); }

 private:
  WordCountTable word_table_;
};

int main(int argc, char** argv) {
  PipelineMain pm(&argc, &argv);

  std::vector<string> inputs;
  for (int i = 1; i < argc; ++i) {
    inputs.push_back(argv[i]);
  }
  CHECK(!inputs.empty());

  hs_compile_error_t* hs_error = nullptr;
  hs_database_t* db = nullptr;
  hs_error_t err = hs_compile(FLAGS_pattern.data(), HS_FLAG_UCP | HS_FLAG_SOM_LEFTMOST,
                              HS_MODE_BLOCK, nullptr, &db, &hs_error);
  CHECK_EQ(HS_SUCCESS, err) << "Error compiling pattern: (" << hs_error->expression;
  CHECK(db);
  hs_free_compile_error(hs_error);

  Pipeline* pipeline = pm.pipeline();

  // Mapper phase
  PTable<WordCount> intermediate_table =
      pipeline->ReadText("inp1", inputs).Map<WordSplitter>("word_splitter", db);
  intermediate_table.Write("word_interim", pb::WireFormat::TXT)
      .WithModNSharding(FLAGS_num_shards,
                        [](const WordCount& wc) { return base::Fingerprint(wc.word); })
      .AndCompress(pb::Output::ZSTD, FLAGS_compress_level);

  // GroupBy phase
  PTable<WordCount> word_counts = pipeline->Join<WordGroupBy>(
      "group_by", {intermediate_table.BindWith(&WordGroupBy::OnWordCount)});
  word_counts.Write("wordcounts", pb::WireFormat::TXT)
      .AndCompress(pb::Output::ZSTD, FLAGS_compress_level);

  LocalRunner* runner = pm.StartLocalRunner(FLAGS_dest_dir);

  pipeline->Run(runner);
  LOG(INFO) << "After pipeline run";

  hs_free_database(db);

  return 0;
}
