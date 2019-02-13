// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "base/init.h"
#include "base/logging.h"

#include "mr/mr3.pb.h"
#include "util/asio/io_context_pool.h"
#include "util/status.h"

using namespace std;

namespace mr3 {

class OutputBase {
  pb::Output output_;

 public:
};

class InputBase {
 public:
  InputBase(const InputBase&) = delete;

  InputBase(const string& name, pb::WireFormat::Type type) {
    input_.set_name(name);
    input_.mutable_format()->set_type(type);
  }

  pb::Input* data() { return &input_; }

  void operator=(const InputBase&) = delete;
 protected:
  pb::Input input_;
};

template <typename T> class Output : public OutputBase {
 public:
  Output& WithSharding(std::function<string(const T&)> cb);
  Output& AndCompress(pb::Output::CompressType ct, unsigned level = 0);
};

template <typename T> class Stream {
  Output<T> out_;
  pb::Operator op_;

 public:
  Stream(const string& input_name) {
    op_.add_input_name(input_name);
  }

  Output<T>& Write(const string& name) { return out_; }
};

using StringStream = Stream<std::string>;

class Pipeline {
 public:
  StringStream& ReadText(const string& name, const string& glob);


  util::Status Run();
 private:
  std::vector<std::unique_ptr<InputBase>> inputs_;

  std::vector<std::unique_ptr<StringStream>> streams_;
};

StringStream& Pipeline::ReadText(const string& name, const string& glob) {
  std::unique_ptr<InputBase> ib(new InputBase(name, pb::WireFormat::TXT));
  ib->data()->add_file_spec()->set_url_glob(glob);
  inputs_.emplace_back(std::move(ib));

  streams_.emplace_back(new StringStream(name));
  auto& ptr = streams_.back();

  return *ptr;
}

template <typename T> Output<T>& Output<T>::WithSharding(std::function<string(const T&)> cb) {
  return *this;
}


template <typename T>
Output<T>& Output<T>::AndCompress(pb::Output::CompressType ct, unsigned level) {
  return *this;
}

using util::Status;

Status Pipeline::Run() {
  return Status::OK;
}

}  // namespace mr3

using namespace mr3;
int main(int argc, char** argv) {
  MainInitGuard guard(&argc, &argv);

  Pipeline p;

  StringStream& ss = p.ReadText("inp1", "*.csv.gz");
  ss.Write("outp1")
      .AndCompress(pb::Output::GZIP)
      .WithSharding([](const string& str) { return "shardname"; });

  return 0;
}
