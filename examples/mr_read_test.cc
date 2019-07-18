// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "base/init.h"
#include "base/logging.h"

#include "mr/local_runner.h"
#include "mr/mr_main.h"
#include "mr/pipeline.h"

using namespace std;
using namespace boost;
using namespace util;

DEFINE_string(dest_dir, "~/mr_output", "");

using namespace mr3;
using namespace util;

class NoOpMapper {
 public:
  void Do(string val, mr3::DoContext<string>* context) {}
};

int main(int argc, char** argv) {
  PipelineMain pm(&argc, &argv);

  std::vector<string> inputs;
  for (int i = 1; i < argc; ++i) {
    inputs.push_back(argv[i]);
  }
  CHECK(!inputs.empty());

  Pipeline* pipeline = pm.pipeline();

  StringTable ss = pipeline->ReadText("inp1", inputs);
  StringTable out_table = ss.Map<NoOpMapper>("map");
  out_table.Write("outp1", pb::WireFormat::TXT)
      .WithModNSharding(10, [](const string& s) { return 0; })
      .AndCompress(pb::Output::ZSTD, 1);

  LocalRunner* runner = pm.StartLocalRunner(FLAGS_dest_dir);

  pipeline->Run(runner);
  LOG(INFO) << "After pipeline run";

  return 0;
}
