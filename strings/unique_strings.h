// Copyright 2013, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#ifndef UNIQUE_STRINGS_H
#define UNIQUE_STRINGS_H

#include <unordered_map>
#include <unordered_set>

#include <sparsehash/dense_hash_map>

#include "base/arena.h"
#include "base/counting_allocator.h"
#include "strings/stringpiece.h"
#include "strings/hash.h"


class UniqueStrings {
public:
  typedef std::unordered_set<
      StringPiece, std::hash<StringPiece>,
      std::equal_to<StringPiece>, base::counting_allocator<StringPiece>> SSet;
  typedef SSet::const_iterator const_iterator;

  StringPiece Get(StringPiece source) {
    return Insert(source).first;
  }

  // returns true if insert took place or false if source was already present.
  // In any case returns the StringPiece from the set.
  std::pair<StringPiece, bool> Insert(StringPiece source);

  size_t MemoryUsage() const {
    return arena_.MemoryUsage() +  db_.get_allocator().bytes_allocated(); // db_.size()*sizeof(StringPiece);
  }

  void Clear() {
    db_.clear();
    base::Arena().Swap(arena_);
  }

  const_iterator begin() { return db_.begin(); }
  const_iterator end() { return db_.end(); }

private:
  base::Arena arena_;
  SSet db_;
};

template<typename M> class ArenaMapBase {
protected:
  typedef M SMap;
public:
  ArenaMapBase() {}

  typedef typename SMap::iterator iterator;
  typedef typename SMap::const_iterator const_iterator;
  typedef typename SMap::value_type value_type;
  typedef typename SMap::mapped_type mapped_type;

  iterator begin() { return map_.begin(); }
  const_iterator begin() const { return map_.begin(); }

  iterator end() { return map_.end(); }
  const_iterator end() const { return map_.end(); }

  iterator find(StringPiece id) { return map_.find(id); }
  const_iterator find(StringPiece id) const { return map_.find(id); }

  void swap(ArenaMapBase& other) {
    map_.swap(other.map_);
    arena_.Swap(other.arena_);
  }

  size_t size() const { return map_.size(); }

  bool empty() const { return map_.empty(); }

  void clear() {
    map_.clear();
    base::Arena().Swap(arena_);
  }

protected:
  base::Arena arena_;
  M map_;

  StringPiece AllocateStr(StringPiece inp) {
    if (inp.empty())
      return inp;
    char* str = arena_.Allocate(inp.size());
    memcpy(str, inp.data(), inp.size());
    return StringPiece(str, inp.size());
  }
};

template<typename T> class StringPieceMap
    : public ArenaMapBase<std::unordered_map<StringPiece, T>> {
  typedef ArenaMapBase<std::unordered_map<StringPiece, T>> Parent;
public:
  using typename Parent::value_type;
  using Parent::map_;

  std::pair<typename StringPieceMap::iterator, bool> insert(
      const typename StringPieceMap::value_type& val) {
    auto it = map_.find(val.first);
    if (it != map_.end())
      return std::make_pair(it, false);
    if (val.first.empty()) {
      it = map_.emplace(StringPiece(), val.second).first;
    } else {
      it = map_.emplace(AllocateStr(val.first), val.second).first;
    }
    return std::make_pair(it, true);
  }

  std::pair<typename StringPieceMap::iterator, bool> emplace(StringPiece key, T&& val) {
    auto it = map_.find(key);
    if (it != map_.end())
      return std::make_pair(it, false);
    it = map_.emplace(this->AllocateStr(key), std::move(val)).first;

    return std::make_pair(it, true);
  }

  std::pair<typename StringPieceMap::iterator, bool> emplace(StringPiece key, const T& val) {
    auto it = map_.find(key);
    if (it != map_.end())
      return std::make_pair(it, false);
    it = map_.emplace(this->AllocateStr(key), val).first;

    return std::make_pair(it, true);
  }

  T& operator[](StringPiece val) {
    auto res = emplace(val, T());
    return res.first->second;
  }

  size_t MemoryUsage() const {
    return this->arena_.MemoryUsage() + map_.size()*sizeof(value_type);
  }
};

template<typename T> class StringPieceDenseMap
    : public ArenaMapBase< ::google::dense_hash_map<StringPiece, T>> {
  typedef ArenaMapBase< ::google::dense_hash_map<StringPiece, T>> Parent;
public:
  using typename Parent::value_type;

  StringPieceDenseMap() {
    // Can not call it because dense_hash_map does not allow calling set_empty_key
    // multiple times.
    // set_empty_key(StringPiece());
  }

  void set_empty_key(StringPiece key) {
    Parent::map_.set_empty_key(key);
  }

  std::pair<typename StringPieceDenseMap::iterator, bool> insert(
      const typename StringPieceDenseMap::value_type& val) {
    auto it = Parent::map_.find(val.first);
    if (it != Parent::map_.end())
      return std::make_pair(it, false);
    it = Parent::map_.insert(value_type(Parent::AllocateStr(val.first), val.second)).first;
    return std::make_pair(it, true);
  }

  T& operator[](StringPiece val) {
    auto res = insert(value_type(val, T()));
    return res.first->second;
  }

  std::pair<typename StringPieceDenseMap::iterator, bool> emplace(StringPiece key, const T& val) {
    return insert(value_type(key, val));
  }

  size_t MemoryUsage() const {
    return this->arena_.MemoryUsage() + this->map_.bucket_count()*sizeof(value_type);
  }

};


#endif  // UNIQUE_STRINGS_H
