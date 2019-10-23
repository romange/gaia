// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
// Parses WET files https://commoncrawl.org/the-data/get-started/#WET-Format from
// common crawl dataset. For example, check out file
// s3://commoncrawl/crawl-data/CC-MAIN-2018-22/segments/1526794863277.18/wet/CC-MAIN-20180520092830-20180520112830-00000.warc.wet.gz
#include <re2/re2.h>

#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
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

  bool Parse(bool is_binary, std::string&& tmp, WordCount* res) { return true; }
};
}  // namespace mr3

/**
 * @brief Parses WARC format.
 *
 */
class WordSplitter {
 public:
  WordSplitter();
  void Do(string did, DoContext<WordCount>* cntx);

  void OnShardFinish(DoContext<WordCount>* cntx);

 private:
  void FlushData(DoContext<WordCount>* cntx);

  StringPieceDenseMap<uint64_t> word_cnts_;

  absl::optional<RE2> re_;
};

WordSplitter::WordSplitter() {
  re_.emplace(R"((\p{L}+))");
  word_cnts_.set_empty_key(StringPiece());
}

void WordSplitter::FlushData(DoContext<WordCount>* cntx) {
  for (const auto& k_v : word_cnts_) {
    cntx->Write(WordCount{string{k_v.first}, k_v.second});
  }
  word_cnts_.clear();
}

void WordSplitter::Do(string line, DoContext<WordCount>* cntx) {
  word_cnts_["foo"]++;
  if (word_cnts_.size() > 200000 && word_cnts_.MemoryUsage() > 256 * 1000000ULL) {
    FlushData(cntx);
  }
}

void WordSplitter::OnShardFinish(DoContext<WordCount>* cntx) {
  if (!word_cnts_.empty()) {
    FlushData(cntx);
  }
}
 
int main(int argc, char** argv) {
  PipelineMain pm(&argc, &argv);

  std::vector<string> inputs;
  for (int i = 1; i < argc; ++i) {
    inputs.push_back(argv[i]);
  }
  CHECK(!inputs.empty());

  Pipeline* pipeline = pm.pipeline();

  PTable<WordCount> word_cnts =
      pipeline->ReadText("inp1", inputs).Map<WordSplitter>("word_splitter");
  auto& outp = word_cnts.Write("wordcnts", pb::WireFormat::TXT)
                   .WithModNSharding(FLAGS_num_shards, [](const WordCount& wc) {
                     return base::Fingerprint(wc.word);
                   });
  outp.AndCompress(pb::Output::ZSTD, 1);

  LocalRunner* runner = pm.StartLocalRunner(FLAGS_dest_dir);

  pipeline->Run(runner);
  LOG(INFO) << "After pipeline run";

  return 0;
}
