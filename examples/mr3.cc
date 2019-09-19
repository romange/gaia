// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#define XXH_STATIC_LINKING_ONLY
#include <xxhash.h>

#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "base/init.h"
#include "base/logging.h"

#include "file/file_util.h"
#include "file/filesource.h"
#include "mr/local_runner.h"
#include "mr/mr_main.h"
#include "mr/pipeline.h"

#include "util/asio/accept_server.h"
#include "util/asio/io_context_pool.h"

using namespace std;
using namespace boost;
using namespace util;

DEFINE_string(compress, "gzip", "can be '', 'gzip' or 'zstd'");
DEFINE_string(dest_dir, "~/mr_output", "");
DEFINE_uint32(num_shards, 10, "");
DEFINE_int32(compress_level, 1, "");

using namespace mr3;
using namespace util;

string ShardNameFunc(const std::string& line) {
  absl::Dec dec(XXH3_64bits(line.data(), line.size()) % FLAGS_num_shards, absl::kZeroPad4);
  return absl::StrCat("shard-", dec);
}

int main(int argc, char** argv) {
  PipelineMain pm(&argc, &argv);

  std::vector<string> inputs;
  for (int i = 1; i < argc; ++i) {
    inputs.push_back(argv[i]);
  }
  CHECK(!inputs.empty());

  Pipeline* pipeline = pm.pipeline();

  StringTable ss = pipeline->ReadText("inp1", inputs);
  auto& outp = ss.Write("outp1", pb::WireFormat::TXT).WithCustomSharding(ShardNameFunc);
  if (!FLAGS_compress.empty()) {
    if (FLAGS_compress == "gzip")
      outp.AndCompress(pb::Output::GZIP);
    else if (FLAGS_compress == "zstd") {
      outp.AndCompress(pb::Output::ZSTD, FLAGS_compress_level);
    } else {
      LOG(FATAL) << "Unknown compress argument " << FLAGS_compress;
    }
  }

  LocalRunner* runner = pm.StartLocalRunner(FLAGS_dest_dir);

  pipeline->Run(runner);
  LOG(INFO) << "After pipeline run";

  return 0;
}
