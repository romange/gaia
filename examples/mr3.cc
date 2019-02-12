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

template <typename T> class Stream {
  Output<T> out_;

 public:
  Output<T>& Write(const string& name) { return out_; }
};

using StringStream = Stream<std::string>;

class Pipeline {
 public:
  StringStream& ReadText(const string& name, const string& glob);

 private:
  std::vector<std::unique_ptr<StringStream>> streams_;
};

StringStream& Pipeline::ReadText(const string& name, const string& glob) {
  streams_.emplace_back(new StringStream());
  return *streams_.back();
}

template <typename T> Output<T>& Output<T>::WithSharding(std::function<string(const T&)> cb) {
  return *this;
}

template <typename T>
Output<T>& Output<T>::AndCompress(pb::Output::CompressType ct, unsigned level) {
  return *this;
}

}  // namespace mr3

using namespace mr3;
int main(int argc, char** argv) {
  MainInitGuard guard(&argc, &argv);

  Pipeline p;
  
  p.ReadText("inp1", "*.csv.gz")
      .Write("outp1")
      .AndCompress(pb::Output::GZIP)
      .WithSharding([](const string& str) { return "shardname"; });

  return 0;
}
