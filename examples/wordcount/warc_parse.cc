// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include <re2/re2.h>

#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "base/hash.h"
#include "base/init.h"
#include "base/logging.h"

#include "file/file_util.h"
#include "absl/strings/strip.h"

#include "mr/local_runner.h"
#include "mr/mr_main.h"
#include "mr/pipeline.h"

using namespace std;
using namespace boost;
using namespace util;

DEFINE_string(dest_dir, "~/mr_output", "");
DEFINE_uint32(num_shards, 10, "");
DEFINE_int32(compress_level, 1, "");

using namespace mr3;
using namespace util;
using re2::RE2;

/**
 * @brief Parses WARC format.
 *
 */
class WarcMapper {
 public:
  WarcMapper();
  void Do(string did, mr3::DoContext<string>* cntx);

 private:
  bool ParseHeader(StringPiece str);
  void HandlePage(string str, mr3::DoContext<string>* cntx);

  enum { INIT, START, PAGE_START, PAGE_CONT } state_ = INIT;
  size_t content_len_ = 0, cur_page_len_ = 0, empty_cnt_ = 0;
  string doc_;
  absl::optional<RE2> re_;
};

WarcMapper::WarcMapper() {
  re_.emplace(R"([^\w\p{L}\p{P}])");
}

void WarcMapper::Do(string line, mr3::DoContext<string>* cntx) {
  switch (state_) {
    case INIT:
      if (line == "WARC/1.0") {
        state_ = START;
      }
      break;
    case START:
      if (!ParseHeader(line)) {
        cntx->raw()->Inc("bad-header");
        LOG(WARNING) << "Bad header " << line;
        state_ = INIT;
      }
      break;
    case PAGE_START:
      CHECK(line.empty()) << line;
      cur_page_len_ = empty_cnt_ = 0;
      state_ = PAGE_CONT;
      break;
    case PAGE_CONT:
      HandlePage(std::move(line), cntx);
      break;
  }
}

bool WarcMapper::ParseHeader(StringPiece str) {
  size_t pos = str.find(':');
  if (pos == StringPiece::npos)
    return false;

  StringPiece name = str.substr(0, pos);
  StringPiece val = absl::StripAsciiWhitespace(str.substr(pos + 1));

  if (name == "WARC-Type") {
    if (val != "conversion") {
      state_ = INIT;  // Ignore non-conversion records.
    }
  } else if (name == "Content-Length") {
    CHECK(absl::SimpleAtoi(val, &content_len_));
    state_ = PAGE_START;
  }
  return true;
}

void WarcMapper::HandlePage(string str, mr3::DoContext<string>* cntx) {
  if (str.empty()) {
    ++empty_cnt_;
    return;
  }
  if (str == "WARC/1.0") {  // reset
    state_ = START;
    LOG_IF(INFO, empty_cnt_ != 2) << "Empty lines: " << empty_cnt_;

    LOG_IF(INFO, cur_page_len_ != content_len_)
        << "Finished content: " << cur_page_len_ << "/" << content_len_;
  } else {
    cur_page_len_ += str.size();
    RE2::GlobalReplace(&str, *re_, " ");
    cntx->Write(std::move(str));  // Write doc line
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

  StringTable ss = pipeline->ReadText("inp1", inputs).Map<WarcMapper>("warc_extract");
  auto& outp = ss.Write("outp1", pb::WireFormat::TXT)
                   .WithModNSharding(FLAGS_num_shards,
                                     [](const string& str) { return base::Fingerprint(str); });
  outp.AndCompress(pb::Output::GZIP);

  LocalRunner* runner = pm.StartLocalRunner(FLAGS_dest_dir);

  pipeline->Run(runner);
  LOG(INFO) << "After pipeline run";

  return 0;
}
