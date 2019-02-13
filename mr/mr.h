// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include <functional>

#include "mr/mr3.pb.h"
#include "util/status.h"

namespace mr3 {

class OutputBase {
  pb::Output output_;

 public:
};

class InputBase {
 public:
  InputBase(const InputBase&) = delete;

  InputBase(const std::string& name, pb::WireFormat::Type type) {
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
  Output& WithSharding(std::function<std::string(const T&)> cb);
  Output& AndCompress(pb::Output::CompressType ct, unsigned level = 0);
};

template <typename T> class Stream {
  Output<T> out_;
  pb::Operator op_;

 public:
  Stream(const std::string& input_name) { op_.add_input_name(input_name); }

  Output<T>& Write(const std::string& name) { return out_; }
};

using StringStream = Stream<std::string>;

class Pipeline {
 public:
  StringStream& ReadText(const std::string& name, const std::string& glob);

  util::Status Run();

  const InputBase& input(const std::string& name);

 private:
  std::vector<std::unique_ptr<InputBase>> inputs_;

  std::vector<std::unique_ptr<StringStream>> streams_;
};

template <typename T> Output<T>& Output<T>::WithSharding(std::function<std::string(const T&)> cb) {
  return *this;
}

template <typename T>
Output<T>& Output<T>::AndCompress(pb::Output::CompressType ct, unsigned level) {
  return *this;
}

}  // namespace mr3
