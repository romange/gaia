// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "base/init.h"
#include "base/logging.h"

#include "mr/mr3.pb.h"
#include "util/asio/io_context_pool.h"

using namespace std;

namespace mr3 {

class OutputBase {
  pb::Output output_;
 public:
};

template <typename T> class Output : public OutputBase {
 public:
  Output& WithSharding(std::function<string(const T&)> cb);
  Output& AndCompress(pb::Output::CompressType ct, unsigned level = 0);
};

class Pipeline {
 public:
  Pipeline& ReadText(const string& name, const string& glob);

  template <typename T> Output<T>& Write(const string& name);
};

}  // namespace mr3


using namespace mr3;
int main(int argc, char** argv) {
  MainInitGuard guard(&argc, &argv);

  Pipeline p;
  p.ReadText("inp1", "*.csv.gz").Write<string>("outp1").AndCompress(pb::Output::GZIP).WithSharding(
    [] (const string& str) {
      return "shardname";
    });

  return 0;
}
