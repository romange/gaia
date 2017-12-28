// Copyright 2013, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#ifndef _META_MAP_BLOCK_H
#define _META_MAP_BLOCK_H

#include <map>
#include <string>
#include "util/status.h"
#include "strings/stringpiece.h"

namespace file {

class MetaMapBlock {
  std::map<std::string, std::string> meta_;

public:
  void Add(const std::string& k, const std::string& v)  {
    meta_[k] = v;
  }

  const std::map<std::string, std::string>& meta() const { return meta_; }

  // Encodes the MetaMapBlock and appends its serialized contents to dest.
  void EncodeTo(std::string* dest) const;

  util::Status DecodeFrom(StringPiece input);

  bool empty() const { return meta_.empty(); }
};

}  // namespace file

#endif  // _META_MAP_BLOCK_H