// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
// Example MR that processes GSOD weather dataset:
// https://cloud.google.com/bigquery/public-data/

#include "base/init.h"
#include "base/logging.h"

#include "file/file_util.h"
#include "mr/local_runner.h"
#include "mr/mr_main.h"

#include "absl/strings/str_cat.h"
#include "strings/split.h"
#include "util/asio/accept_server.h"

using namespace mr3;
using namespace util;
using namespace std;

DEFINE_string(dest_dir, "~/mr_output", "");

struct GsodRecord {
  uint32_t station;
  int year;
};

template <> class mr3::RecordTraits<GsodRecord> {
  std::vector<char*> cols_;

 public:
  static std::string Serialize(GsodRecord&& rec) {
    return absl::StrCat(rec.station, ",", rec.year);
  }

  bool Parse(std::string&& tmp, GsodRecord* res) {
    cols_.clear();

    SplitCSVLineWithDelimiter(&tmp.front(), ',', &cols_);
    CHECK_EQ(2, cols_.size());
    CHECK(absl::SimpleAtoi(cols_[0], &res->station));
    CHECK(absl::SimpleAtoi(cols_[1], &res->year));

    return true;
  }
};

class GsodMapper {
  std::vector<char*> cols_;

 public:
  void Do(string val, mr3::DoContext<GsodRecord>* context) {
    cols_.clear();
    string tmp = val;
    SplitCSVLineWithDelimiter(&val.front(), ',', &cols_);
    CHECK_EQ(31, cols_.size()) << tmp;

    GsodRecord rec;
    CHECK(absl::SimpleAtoi(cols_[0], &rec.station));
    CHECK(absl::SimpleAtoi(cols_[2], &rec.year));
    context->Write(std::move(rec));
  }
};

class GsodJoiner {
  absl::flat_hash_map<unsigned, uint32_t> cnt_;
 public:
  void Group(GsodRecord record, mr3::DoContext<string>* context) {
    cnt_[record.year]++;
  }

  void OnShardFinish(DoContext<string>* cntx) {
    cntx->Write("year,count");
    for (const auto& k_v : cnt_) {
      cntx->Write(absl::StrCat(k_v.first, ",", k_v.second));
    }
    cnt_.clear();
  }
};

int main(int argc, char** argv) {
  PipelineMain pm(&argc, &argv);

  std::vector<string> inputs;
  for (int i = 1; i < argc; ++i) {
    inputs.push_back(argv[i]);
  }
  CHECK(!inputs.empty());

  LocalRunner runner(file_util::ExpandPath(FLAGS_dest_dir));

  Pipeline& pipeline = *pm.pipeline();
  pm.accept_server()->TriggerOnBreakSignal([&] {
    pipeline.Stop();
    runner.Stop();
  });

  StringTable ss = pipeline.ReadText("gsod", inputs);
  pipeline.mutable_input("gsod")->set_skip_header(1);
  PTable<GsodRecord> records = ss.Map<GsodMapper>("MapToGsod");
  records.Write("gsod_map", pb::WireFormat::TXT)
      .WithModNSharding(10, [](const GsodRecord& r) { return r.year; })
      .AndCompress(pb::Output::GZIP);

  StringTable joined =
      pipeline.Join<GsodJoiner>("group_by", {records.BindWith(&GsodJoiner::Group)});
  joined.Write("joined_table", pb::WireFormat::TXT)
      .AndCompress(pb::Output::GZIP);

  pipeline.Run(&runner);
  LOG(INFO) << "After pipeline run";

  return 0;
}
