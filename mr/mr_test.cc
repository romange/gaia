// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include <gmock/gmock.h>

#include <rapidjson/error/en.h>
#include <rapidjson/writer.h>

#include "base/gtest.h"
#include "base/logging.h"
#include "file/file_util.h"
#include "mr/mr_pb.h"
#include "mr/pipeline.h"
#include "mr/test_utils.h"

#include "strings/numbers.h"
#include "util/asio/io_context_pool.h"
#include "util/plang/addressbook.pb.h"

using namespace std;

namespace mr3 {

using namespace util;
using namespace boost;
using other::StrVal;
using testing::Contains;
using testing::ElementsAre;
using testing::Pair;
using testing::UnorderedElementsAre;
using testing::UnorderedElementsAreArray;

namespace rj = rapidjson;

struct MetaCheck {
  set<string> input_files;
  set<string> meta;
  vector<size_t> pos;
};

class StrValMapper {
  MetaCheck* meta_check_;
 public:
  StrValMapper(MetaCheck* meta_check) : meta_check_(meta_check) {}

  void Do(string val, mr3::DoContext<StrVal>* cntx) {
    meta_check_->input_files.insert(cntx->raw()->input_file_name());
    if (absl::holds_alternative<int64_t>(cntx->raw()->meta_data())) {
      meta_check_->meta.insert(to_string(absl::get<int64_t>(cntx->raw()->meta_data())));
    }
    meta_check_->pos.push_back(cntx->raw()->input_pos());

    StrVal a;
    val.append("a");
    a.val = std::move(val);

    cntx->Write(std::move(a));
  }
};

struct IntVal {
  int val;

  operator string() const { return std::to_string(val); }
};

template <> class RecordTraits<IntVal> {
 public:
  static std::string Serialize(bool is_binary, const IntVal& doc) { return absl::StrCat(doc.val); }

  bool Parse(bool is_binary, std::string&& tmp, IntVal* res) {
    return safe_strto32(tmp, &res->val);
  }
};

class MrTest : public testing::Test {
 protected:
  void SetUp() final {
    pool_.reset(new IoContextPool{1});
    pool_->Run();
    pipeline_.reset(new Pipeline(pool_.get()));
  }

  void TearDown() final { pool_.reset(); }

  auto MatchShard(const string& sh_name, const vector<string>& elems) {
    // matcher references the array, so we must to store it in case we pass a temporary rvalue.
    tmp_.emplace_back(new vector<string>{elems});

    return Pair(ShardId{sh_name}, UnorderedElementsAreArray(*tmp_.back()));
  }

  auto MatchShard(unsigned index, const vector<string>& elems) {
    // matcher references the array, so we must to store it in case we pass a temporary rvalue.
    tmp_.emplace_back(new vector<string>{elems});

    return Pair(ShardId{index}, UnorderedElementsAreArray(*tmp_.back()));
  }

  std::unique_ptr<IoContextPool> pool_;
  std::unique_ptr<Pipeline> pipeline_;
  TestRunner runner_;

  vector<std::unique_ptr<vector<string>>> tmp_;
};

TEST_F(MrTest, Basic) {
  EXPECT_EQ(ShardId{1}, ShardId{1});
  EXPECT_NE(ShardId{1}, ShardId{"foo"});

  StringTable str1 = pipeline_->ReadText("read_bar", "bar.txt");

  str1.Write("new_table", pb::WireFormat::TXT)
      .AndCompress(pb::Output::GZIP, 1)
      .WithCustomSharding([](const std::string& rec) { return "shard1"; });

  vector<string> elements{"1", "2", "3", "4"};

  runner_.AddInputRecords("bar.txt", elements);
  pipeline_->Run(&runner_);

  EXPECT_THAT(runner_.Table("new_table"), ElementsAre(MatchShard("shard1", elements)));
  EXPECT_EQ("fn-calls,4\n"
            "fn-writes,4\n"
            "map-input-read_bar,4\n"
            "parse-errors,0\n",
            runner_.SavedFile(file_util::JoinPath("new_table", "counter_map.csv")));
}

TEST_F(MrTest, Json) {
  PTable<rapidjson::Document> json_table = pipeline_->ReadText("read_bar", "bar.txt").AsJson();
  auto json_shard_func = [](const rapidjson::Document& doc) {
    return doc.HasMember("foo") ? "shard0" : "shard1";
  };

  json_table.Write("json_table", pb::WireFormat::TXT)
      .AndCompress(pb::Output::GZIP, 1)
      .WithCustomSharding(json_shard_func);

  const char kJson1[] = R"({"foo":"bar"})";
  const char kJson2[] = R"({"id":1})";
  const char kJson3[] = R"({"foo":null})";

  vector<string> elements{kJson2, kJson1, kJson3};

  runner_.AddInputRecords("bar.txt", elements);
  pipeline_->Run(&runner_);
  EXPECT_THAT(
      runner_.Table("json_table"),
      UnorderedElementsAre(MatchShard("shard0", {kJson3, kJson1}), MatchShard("shard1", {kJson2})));
}

TEST_F(MrTest, InvalidJson) {
  char str[] = R"({"roman":"��i���u�.nW��'$��uٿ�����d�ݹ��5�"} )";

  rapidjson::Document doc;
  doc.Parse(str);

  rj::StringBuffer s;
  rj::Writer<rj::StringBuffer> writer(s);
  doc.Accept(writer);
  LOG(INFO) << s.GetString();
}

TEST_F(MrTest, ParseError) {
  runner_.AddInputRecords("bar.txt", {"a", "b", "c", "d"});
  PTable<IntVal> itable = pipeline_->ReadText("read_bar", "bar.txt").As<IntVal>();
  itable.Write("table", pb::WireFormat::TXT).WithModNSharding(10, [](const IntVal& iv) {
    return iv.val;
  });
  pipeline_->Run(&runner_);
  EXPECT_EQ(0, runner_.write_calls);
  EXPECT_EQ(4, runner_.parse_errors);
}

TEST_F(MrTest, Map) {
  pb::Input::FileSpec fspec;
  fspec.set_url_glob("bar.txt");
  fspec.set_i64val(42);

  StringTable str1 = pipeline_->ReadText("read_bar", std::vector<pb::Input::FileSpec>{fspec});

  MetaCheck meta_check;
  PTable<StrVal> str2 = str1.Map<StrValMapper>("Map1", &meta_check);

  str2.Write("table", pb::WireFormat::TXT).WithModNSharding(10, [](const StrVal&) { return 11; });
  vector<string> elements{"1", "2", "3", "4"};

  runner_.AddInputRecords("bar.txt", elements);
  pipeline_->Run(&runner_);

  EXPECT_THAT(meta_check.input_files, UnorderedElementsAre("bar.txt"));
  EXPECT_THAT(meta_check.meta, UnorderedElementsAre("42"));
  EXPECT_THAT(meta_check.pos, ElementsAre(0, 1, 2, 3));

  vector<string> expected;
  for (const auto& e : elements)
    expected.push_back(e + "a");

  EXPECT_THAT(runner_.Table("table"), ElementsAre(MatchShard(1, expected)));
}

class IntMapper {
 public:
  void Do(StrVal a, mr3::DoContext<IntVal>* cntx) {
    CHECK_GT(a.val.size(), 1);
    a.val.pop_back();

    IntVal iv;
    CHECK(safe_strto32(a.val, &iv.val)) << a.val;
    cntx->Write(iv);
    auto& map = cntx->raw()->GetFreqMapStatistic<int>("int_map");
    ++map[iv.val];
  }
};

TEST_F(MrTest, MapAB) {
  vector<string> elements{"1", "2", "3", "4"};

  runner_.AddInputRecords("bar.txt", elements);
  PTable<IntVal> itable =
      pipeline_->ReadText("read_bar", "bar.txt").As<IntVal>();  // Map<StrValMapper>("Map1");

  MetaCheck meta_check;
  PTable<StrVal> atable = itable.Map<StrValMapper>("Map1", &meta_check);

  atable.Write("table", pb::WireFormat::TXT).WithModNSharding(10, [](const StrVal&) { return 11; });

  PTable<IntVal> final_table = atable.Map<IntMapper>("IntMap");
  final_table.Write("final_table", pb::WireFormat::TXT).WithModNSharding(7, [](const IntVal&) {
    return 10;
  });

  pipeline_->Run(&runner_);
  EXPECT_THAT(meta_check.input_files, UnorderedElementsAre("bar.txt"));

  vector<string> expected;
  for (const auto& e : elements)
    expected.push_back(e + "a");

  EXPECT_THAT(runner_.Table("table"), ElementsAre(MatchShard(1, expected)));
  EXPECT_THAT(runner_.Table("final_table"), ElementsAre(MatchShard(3, elements)));
  auto* int_map = pipeline_->GetFreqMap<int>("int_map");
  ASSERT_TRUE(int_map);
  EXPECT_THAT(*int_map, UnorderedElementsAre(Pair(1, 1), Pair(2, 1), Pair(3, 1), Pair(4, 1)));
}

class StrJoiner {
  absl::flat_hash_map<int, int> counts_;

 public:
  void On1(IntVal&& iv, DoContext<string>* out) { counts_[iv.val]++; }

  void On2(IntVal&& iv, DoContext<string>* out) { counts_[iv.val] += 10; }

  void OnShardFinish(DoContext<string>* cntx) {
    for (const auto& k_v : counts_) {
      cntx->Write(absl::StrCat(k_v.first, ":", k_v.second));
    }
    counts_.clear();
  }
};

TEST_F(MrTest, Join) {
  vector<string> stream1{"1", "2", "3", "4"}, stream2{"2", "3"};

  runner_.AddInputRecords("stream1.txt", stream1);
  runner_.AddInputRecords("stream2.txt", stream2);

  PTable<IntVal> itable1 = pipeline_->ReadText("read1", "stream1.txt").As<IntVal>();
  PTable<IntVal> itable2 = pipeline_->ReadText("read2", "stream2.txt").As<IntVal>();
  itable1.Write("ss1", pb::WireFormat::TXT).WithModNSharding(3, [](const IntVal& iv) {
    return iv.val;
  });

  itable2.Write("ss2", pb::WireFormat::TXT).WithModNSharding(3, [](const IntVal& iv) {
    return iv.val;
  });

  PTable<string> res = pipeline_->Join(
      "join_tables", {itable1.BindWith(&StrJoiner::On1), JoinInput(itable2, &StrJoiner::On2)});

  res.Write("joinw", pb::WireFormat::TXT);

  pipeline_->Run(&runner_);
  EXPECT_THAT(runner_.Table("joinw"),
              UnorderedElementsAre(MatchShard(0, {"3:11"}), MatchShard(1, {"1:1", "4:1"}),
                                   MatchShard(2, {"2:11"})));
}

class GroupByInt {
  absl::flat_hash_map<int, int> counts_;

 public:
  void Add(IntVal iv, DoContext<IntVal>* out) { counts_[iv.val]++; }

  void OnShardFinish(DoContext<IntVal>* cntx) {
    for (const auto& k_v : counts_) {
      IntVal iv;
      iv.val = k_v.second;
      cntx->Write(iv);
    }
    counts_.clear();
  }
};

TEST_F(MrTest, JoinAndReshard) {
  vector<string> stream{"1", "2", "3", "4", "1", "2", "2", "2", "2"};
  runner_.AddInputRecords("stream1.txt", stream);

  PTable<IntVal> itable = pipeline_->ReadText("read1", "stream1.txt").As<IntVal>();
  itable.Write("ss1", pb::WireFormat::TXT).WithModNSharding(3, [](const IntVal& iv) {
    return iv.val;
  });
  PTable<IntVal> joined = pipeline_->Join("join_tables", {itable.BindWith(&GroupByInt::Add)});

  joined.Write("joinw", pb::WireFormat::TXT).WithModNSharding(2, [](const IntVal& iv) {
    return iv.val;
  });
  pipeline_->Run(&runner_);

  EXPECT_THAT(runner_.Table("joinw"),
              UnorderedElementsAre(MatchShard(0, {"2"}), MatchShard(1, {"5", "1", "1"})));
}


TEST_F(MrTest, JoinReduceNoSharding) {
  vector<string> stream{"1", "2", "3", "4", "1", "2", "2", "2", "2"};
  runner_.AddInputRecords("stream1.txt", stream);

  PTable<IntVal> itable = pipeline_->ReadText("read1", "stream1.txt").As<IntVal>();
  itable.Write("ss1", pb::WireFormat::TXT).WithModNSharding(3, [](const IntVal& iv) {
    return iv.val;
  });

  PTable<IntVal> joined = pipeline_->Join("join_tables", {itable.BindWith(&GroupByInt::Add)});
  joined.Write("joinw", pb::WireFormat::TXT);
  pipeline_->Run(&runner_);

  EXPECT_THAT(runner_.Table("joinw"),
              UnorderedElementsAre(MatchShard(0, {"1"}), MatchShard(1, {"2", "1"}),
                                   MatchShard(2, {"5"})));
}

class AddressMapper {
 public:
  void Do(string str, DoContext<tutorial::Address>* out) {
    tutorial::Address addr;
    addr.set_street(str);
    out->Write(std::move(addr));
  }
};

TEST_F(MrTest, PbJson) {
  vector<string> stream1{"1", "2", "3", "4"};
  runner_.AddInputRecords("stream1.txt", stream1);
  PTable<tutorial::Address> table =
      pipeline_->ReadText("read1", "stream1.txt").Map<AddressMapper>("map_address");
  table.Write("w1", pb::WireFormat::TXT).WithCustomSharding([](const tutorial::Address&) {
    return "shard";
  });
  pipeline_->Run(&runner_);

  vector<string> expected;
  for (const auto& s : stream1) {
    expected.push_back(absl::StrCat(R"({"street":")", s, "\"}"));
  }
  EXPECT_THAT(runner_.Table("w1"), UnorderedElementsAre(MatchShard("shard", expected)));
}

TEST_F(MrTest, PbLst) {
  vector<string> stream1{"1", "2"};
  runner_.AddInputRecords("stream1.txt", stream1);
  PTable<tutorial::Address> table =
      pipeline_->ReadText("read1", "stream1.txt").Map<AddressMapper>("map_address");
  table.Write("w1", pb::WireFormat::LST).WithCustomSharding([](const tutorial::Address&) {
    return "shard";
  });
  pipeline_->Run(&runner_);

  vector<string> expected;
  for (const auto& s : stream1) {
    tutorial::Address addr;
    addr.set_street(s);
    expected.push_back(addr.SerializeAsString());
  }
  EXPECT_THAT(runner_.Table("w1"), UnorderedElementsAre(MatchShard("shard", expected)));
}

TEST_F(MrTest, Scope) {
  vector<string> stream1{"1", "2", "3", "4"};
  runner_.AddInputRecords("stream1.txt", stream1);
  {
    PTable<string> table = pipeline_->ReadText("read1", "stream1.txt");
    table.Write("w1", pb::WireFormat::TXT).WithModNSharding(10, [](const auto&) { return 1; });
  }
  pipeline_->Run(&runner_);

  EXPECT_THAT(runner_.Table("w1"), UnorderedElementsAre(MatchShard(1, stream1)));
}

static void BM_ShardAndWrite(benchmark::State& state) {
  IoContextPool pool(1);
  pool.Run();

  std::unique_ptr<Pipeline> pipeline(new Pipeline(&pool));
  PTable<IntVal> itable = pipeline->ReadText("bench_intval", "intval.txt").As<IntVal>();
  itable.Write("out_intval.txt", pb::WireFormat::TXT).WithModNSharding(7, [](const IntVal& iv) {
    return iv.val;
  });

  EmptyRunner er;
  er.gen_fn = [&](string* val) {
    *val = "42";

    return state.KeepRunning();
  };
  pipeline->Run(&er);
}
BENCHMARK(BM_ShardAndWrite);

}  // namespace mr3
