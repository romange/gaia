// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include <boost/fiber/buffered_channel.hpp>

#include "base/init.h"
#include "base/logging.h"

#include "file/file_util.h"
#include "mr/mr3.pb.h"
#include "util/asio/io_context_pool.h"
#include "util/status.h"

using namespace std;
using namespace boost;

DEFINE_string(input, "", "");

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

  pb::Input* mutable_msg() { return &input_; }
  const pb::Input& msg() const { return input_; }

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
  Stream(const string& input_name) { op_.add_input_name(input_name); }

  Output<T>& Write(const string& name) { return out_; }
};

using StringStream = Stream<std::string>;

class Pipeline {
 public:
  StringStream& ReadText(const string& name, const string& glob);

  util::Status Run();

  const InputBase& input(const string& name);

 private:
  std::vector<std::unique_ptr<InputBase>> inputs_;

  std::vector<std::unique_ptr<StringStream>> streams_;
};

StringStream& Pipeline::ReadText(const string& name, const string& glob) {
  std::unique_ptr<InputBase> ib(new InputBase(name, pb::WireFormat::TXT));
  ib->mutable_msg()->add_file_spec()->set_url_glob(glob);
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

Status Pipeline::Run() { return Status::OK; }

class Executor {
  struct PerIoStruct {};

  using StringQueue = ::boost::fibers::buffered_channel<string>;
 public:
  Executor(util::IoContextPool* pool) : pool_(pool), file_name_q_(16) {}

  void Run(const InputBase* input, const StringStream& ss);

 private:
  util::IoContextPool* pool_;
  StringQueue file_name_q_;
  static thread_local std::unique_ptr<PerIoStruct> per_io_;
};

void Executor::Run(const InputBase* input, const StringStream& ss) {
  CHECK(input && input->msg().file_spec_size() > 0);
  // TODO: To setup receiving fibers.

  for (const auto& file_spec : input->msg().file_spec()) {
    std::vector<file_util::StatShort> paths = file_util::StatFiles(file_spec.url_glob());
    for (const auto& v : paths) {
      if (v.st_mode & S_IFREG) {
        fibers::channel_op_status st = file_name_q_.push(v.name);
        CHECK_EQ(fibers::channel_op_status::success, st);
      }
    }
  }
}

}  // namespace mr3

using namespace mr3;
using namespace util;

int main(int argc, char** argv) {
  MainInitGuard guard(&argc, &argv);

  CHECK(!FLAGS_input.empty());

  IoContextPool pool;
  Pipeline p;

  // TODO: Should return Input<string> or something which can apply an operator.
  StringStream& ss = p.ReadText("inp1", FLAGS_input);
  ss.Write("outp1").AndCompress(pb::Output::GZIP).WithSharding([](const string& str) {
    return "shardname";
  });

  return 0;
}
