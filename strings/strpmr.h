// Copyright 2016, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <pmr/monotonic_buffer_resource.h>

#include "base/pmr.h"

#include "strings/hash.h"
#include "strings/stringpiece.h"

namespace strings {

// Deep copies src and returns the result. mr should probably be arena-like memory manager.
StringPiece DeepCopy(StringPiece src, pmr::memory_resource* mr);

// Pool of non-empty strings. Empty strings can not be inserted and should be handled separately
// by the callers of this class.
class StringPieceSet {
public:
  typedef base::pmr_dense_hash_set<StringPiece> SSet;
  typedef SSet::const_iterator const_iterator;

  StringPieceSet(pmr::memory_resource* mr)
      : arena_(mr), db_(0, SSet::hasher(), SSet::key_equal(), mr) {
    db_.set_empty_key(StringPiece());
  }

  StringPiece Get(StringPiece source) {
    return Insert(source).first;
  }

  // returns true if insert took place or false if source was already present.
  // In any case returns the StringPiece from the set.
  std::pair<StringPiece, bool> Insert(StringPiece source);

  const_iterator begin() { return db_.begin(); }
  const_iterator end() { return db_.end(); }

private:
  pmr::monotonic_buffer_resource arena_;
  SSet db_;
};

// Stores map of non-empty array to U values.
template<typename T, typename U> class ArraysMap {
 public:
  typedef strings::Range<const T*> key_type;
  typedef base::pmr_dense_hash_map<key_type, U> Map;
  typedef typename Map::const_iterator const_iterator;

  ArraysMap(pmr::memory_resource* mr)
      : arena_(mr), map_(0, typename Map::hasher(), typename Map::key_equal(), mr) {

    map_.set_empty_key(key_type{});
    static_assert(std::is_pod<U>::value, "");
  }

  const_iterator begin() { return map_.begin(); }
  const_iterator end() { return map_.end(); }

  size_t size() const { return map_.size(); }

  bool empty() const { return map_.empty(); }

  std::pair<const_iterator, bool> emplace(const key_type& key, const U& val) {
    auto it = map_.find(key);
    if (it != map_.end())
      return std::make_pair(it, false);
    it = map_.emplace(AllocateRange(key), val).first;

    return std::make_pair(it, true);
  }

 private:
  key_type AllocateRange(const key_type& k) {
    if (k.empty())
      return k;
    T* val = reinterpret_cast<T*>(arena_.allocate(k.size() * sizeof(T)));
    memcpy(val, k.data(), k.size() * sizeof(T));
    return key_type(val, k.size());
  }
  pmr::monotonic_buffer_resource arena_;
  Map map_;
};

}  // namespace strings
