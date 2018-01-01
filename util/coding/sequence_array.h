// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include "base/pod_array.h"
#include "strings/range.h"

namespace util {

class SequenceArray {
  base::PODArray<uint8> data_;
  std::vector<uint32> len_;

 public:
  class Iterator {
    const uint8* pval_;
    const uint32* plen_;

  public:
    Iterator(const uint8* pv, const uint32_t* pl) : pval_(pv), plen_(pl) {
    }

    strings::ByteRange operator*() const { return strings::ByteRange(pval_, *plen_); }

    Iterator& operator++() {
      pval_ += *plen_;
      ++plen_;
      return *this;
    }

    bool operator==(const Iterator& o) const {
      return pval_ == o.pval_;
    }

    bool operator!=(const Iterator& o) const {
      return !(*this == o);
    }
  };

  typedef Iterator const_iterator;

  template<typename U> uint32 Add(const U* s, const U* e) {
    static_assert(sizeof(U) == 1, "");
    uint32 res = len_.size();
    data_.insert(s, e);
    len_.push_back(e - s);
    return res;
  }

  const base::PODArray<uint8>& data() const { return data_; }

  bool empty() const { return len_.empty(); }
  void reserve(size_t sz) { data_.reserve(sz); }

  size_t data_size() const { return data_.size(); }
  const std::vector<uint32>& len_array() const { return len_; }

  void clear() {
    data_.clear();
    len_.clear();
  }

  const_iterator begin() const { return Iterator(data_.begin(), len_.data()); }
  const_iterator end() const { return Iterator(data_.end(), nullptr); }

  size_t GetMaxSerializedSize() const;
  size_t SerializeTo(uint8* dest) const;
  void SerializeFrom(const uint8_t* src, uint32_t count);
};


}  // namespace util
