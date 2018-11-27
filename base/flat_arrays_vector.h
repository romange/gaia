// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <vector>
#include "base/integral_types.h"

namespace base {

// Represents vector of vectors using flat memory block.
// Useful when you build vector of vectors once and continously use them afterwards.
// The data structure allows to map from dense index to vector items of type T.
template<typename T> class FlatArraysVec {
  std::vector<uint32> offsets_;
  std::vector<T> data_;

public:
  // The auxillary class that allows iterating over elements belonging to data_.
  // This is allowed by implementing begin(), end() methods.
  class RangeWrapper {
    const T* t_;
    uint32 sz_;
  public:
    RangeWrapper(const T* t, uint32 sz) : t_(t), sz_(sz) {}

    const T* begin() const { return t_; }
    const T* end() const { return t_ + sz_; }
  };

  void Add(const std::vector<T>& items) {
    offsets_.push_back(data_.size());  // adds start offset.
    data_.insert(data_.end(), items.begin(), items.end());  // copied the data.
  }

  void Finalize() {
    offsets_.shrink_to_fit();
    data_.shrink_to_fit();
  }

  RangeWrapper range(uint32 index) const {
    assert(index < offsets_.size());
    uint32 start = offsets_[index];
    uint32 end = index+1 < offsets_.size() ? offsets_[index + 1] : data_.size();
    return RangeWrapper(data_.data() + start, end - start);
  }

  // Returns number of arrays in flat array.
  uint32 size() const { return offsets_.size(); }
};

}  // namespace base
