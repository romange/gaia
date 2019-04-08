// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include "mr/mr.h"

#include "util/fibers/simple_channel.h"


namespace util {
class IoContextPool;
}  // namespace util

namespace mr3 {

using RecordQueue = util::fibers_ext::SimpleChannel<std::string>;

class Runner {
 public:
  virtual ~Runner();

  virtual void Init() = 0;

  virtual void Shutdown() = 0;

  virtual void OperatorStart() = 0;

  // Must be thread-safe. Called from multiple threads in pipeline_executor.
  virtual RawContext* CreateContext(const pb::Operator& op) = 0;

  virtual void OperatorEnd() = 0;

  virtual void ExpandGlob(const std::string& glob, std::function<void(const std::string&)> cb) = 0;

  // Read file and fill queue. This function must be fiber-friendly.
  // Returns number of records processed.
  virtual size_t ProcessFile(const std::string& filename,
                             pb::WireFormat::Type type, RecordQueue* queue) = 0;
};

class Pipeline {
  friend class detail::TableBase;
 public:
  explicit Pipeline(util::IoContextPool* pool);
  ~Pipeline();

  StringTable ReadText(const std::string& name, const std::vector<std::string>& globs);

  StringTable ReadText(const std::string& name, const std::string& glob) {
    return ReadText(name, std::vector<std::string>{glob});
  }

  void Run(Runner* runner);

  // Stops/breaks the run.
  void Stop();

 private:
  const InputBase* CheckedInput(const std::string& name) const;

  class Executor;

  util::IoContextPool* pool_;
  std::unordered_map<std::string, std::unique_ptr<InputBase>> inputs_;
  std::vector<boost::intrusive_ptr<detail::TableBase>> tables_;

  std::unique_ptr<Executor> executor_;
};

}  // namespace mr3
