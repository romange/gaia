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

  virtual RawContext* CreateContext() = 0;

  virtual void ExpandGlob(const std::string& glob, std::function<void(const std::string&)> cb) = 0;

  // Read file and fill queue. This function must be fiber-friendly.
  virtual void ProcessFile(const std::string& filename,
                           pb::WireFormat::Type type, RecordQueue* queue) = 0;
};

class Pipeline {
  friend class TableBase;
 public:
  Pipeline();
  ~Pipeline();

  StringTable ReadText(const std::string& name, const std::vector<std::string>& globs);

  StringTable ReadText(const std::string& name, const std::string& glob) {
    return ReadText(name, std::vector<std::string>{glob});
  }

  void Run(util::IoContextPool* pool, Runner* runner);

 private:
  const InputBase* CheckedInput(const std::string& name) const;

  struct Executor;

  std::unordered_map<std::string, std::unique_ptr<InputBase>> inputs_;
  std::vector<boost::intrusive_ptr<TableBase>> tables_;

  std::unique_ptr<Executor> executor_;
};

}  // namespace mr3
