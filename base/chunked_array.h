// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <memory>
#include <pmr/polymorphic_allocator.h>
#include <pmr/vector.h>
#include "base/pmr.h"

namespace base {

template<typename T, size_t CHUNK_SIZE = 256> class ChunkedArray {
 public:
  ChunkedArray(pmr::memory_resource* mr = nullptr);
  ChunkedArray(size_t count, pmr::memory_resource* mr = nullptr);

  ~ChunkedArray() {
    clear();
  }

  T& operator[](size_t i) {
    return chunk_vec_[i / CHUNK_SIZE]->at(i % CHUNK_SIZE);
  }
  const T& operator[](size_t i) const {
    return chunk_vec_[i / CHUNK_SIZE]->at(i % CHUNK_SIZE);
  }

  void clear() {
    if (chunk_vec_.size()) {
      chunk_vec_.back()->destroy(0, CHUNK_SIZE - last_chunk_left_);
      chunk_vec_.pop_back();
      for (auto& x : chunk_vec_)
        x->destroy(0, CHUNK_SIZE);
    }
    chunk_vec_.clear();
    last_chunk_left_ = 0;
  }

  size_t size() const {
    return chunk_vec_.size() * CHUNK_SIZE - last_chunk_left_;
  }

  bool empty() const { return chunk_vec_.empty(); }

  template<typename ...Args> void emplace_back(Args&&... args) {
    if (last_chunk_left_ == 0) {
      AddChunk();
      last_chunk_left_ = CHUNK_SIZE;
    }
    chunk_vec_.back()->construct(CHUNK_SIZE - last_chunk_left_, std::forward<Args>(args)...);

    --last_chunk_left_;
  }

  T& back() {
    return chunk_vec_.back()->at(CHUNK_SIZE - last_chunk_left_ - 1);
  }

  T& front() { return chunk_vec_.front()->at(0); }
  const T& front() const { return chunk_vec_.front()->at(0); }

  ChunkedArray& operator=(const ChunkedArray& other);

 private:
  void AddChunk() {
    pmr::memory_resource* mr = chunk_vec_.get_allocator().resource();
    chunk_vec_.emplace_back(make_pmr_unique<Chunk>(mr));
  }

  struct Chunk {
    typename std::aligned_storage_t<sizeof(T), alignof(T)> data[CHUNK_SIZE];

    void destroy(size_t from, size_t last) {
      for (size_t i = from; i < last; ++i) {
        reinterpret_cast<T&>(data[i]).~T();
      }
    }

    void copy_from(const Chunk& other, size_t cnt) {
      for (size_t i = 0; i < cnt; ++i) {
        reinterpret_cast<T&>(data[i]) = reinterpret_cast<const T&>(other.data[i]);
      }
    }

    template<typename ...Args> void construct(size_t i, Args&&... args) {
      ::new (static_cast<void*>(data + i)) T(std::forward<Args>(args)...);
    }

    T& at(size_t i) { return *reinterpret_cast<T*>(data + i); }
    const T& at(size_t i) const { return *reinterpret_cast<const T*>(data + i); }

    void construct(const Chunk& other, size_t begin, size_t end) {
      for (size_t j = begin; j < end; ++j) {
        construct(j, other.at(j));
      }
    }
  };


  pmr::vector<base::pmr_unique_ptr<Chunk>> chunk_vec_;
  size_t last_chunk_left_ = 0;
};

template<typename T, size_t CHUNK_SIZE>
    ChunkedArray<T, CHUNK_SIZE>::ChunkedArray(pmr::memory_resource* mr)
    : chunk_vec_(mr ? mr : pmr::get_default_resource()) {
}

template<typename T, size_t CHUNK_SIZE>
    ChunkedArray<T, CHUNK_SIZE>::ChunkedArray(size_t count, pmr::memory_resource* mr)
    : ChunkedArray(mr) {
  size_t num_chunks = (count + CHUNK_SIZE - 1) / CHUNK_SIZE;
  pmr::memory_resource* mr2 = chunk_vec_.get_allocator().resource();
  for (size_t i = 0; i < num_chunks; ++i) {
    chunk_vec_.emplace_back(make_pmr_unique<Chunk>(mr2));
  }
  last_chunk_left_ = (CHUNK_SIZE - (count % CHUNK_SIZE)) % CHUNK_SIZE;
}

template<typename T, size_t CHUNK_SIZE>
    ChunkedArray<T, CHUNK_SIZE> & ChunkedArray<T, CHUNK_SIZE>::operator=(
    const ChunkedArray& other) {
  if (other.empty()) {
    clear();
    return *this;
  }

  if (!chunk_vec_.empty()) {  // Handle possible ranges overlap.
    size_t last = CHUNK_SIZE - last_chunk_left_;
    while (chunk_vec_.size() > other.chunk_vec_.size()) {
      chunk_vec_.back()->destroy(0, last);
      last = CHUNK_SIZE;
      chunk_vec_.pop_back();
    }

    // Now chunk_vec_.size() <= other.chunk_vec_. Also chunk_vec_ is not empty.
    size_t index = 0;
    // Copy the existing chunks fully.
    for (; index < chunk_vec_.size() - 1; ++index) {
      chunk_vec_[index]->copy_from(*other.chunk_vec_[index], CHUNK_SIZE);
    }

    if (chunk_vec_.size() == other.chunk_vec_.size()) {
      size_t other_last = CHUNK_SIZE - other.last_chunk_left_;

      if (last >= other_last) {
        chunk_vec_.back()->copy_from(*other.chunk_vec_.back(), other_last);
        chunk_vec_.back()->destroy(other_last, last);
      } else {
        chunk_vec_.back()->copy_from(*other.chunk_vec_.back(), last);
        chunk_vec_.back()->construct(*other.chunk_vec_.back(), last, other_last);
      }
      last_chunk_left_ = other.last_chunk_left_;

      return *this;
    }

    // Here chunk_vec_.size() < other.chunk_vec_.size().
    // We must copy first part and construct the rest.
    chunk_vec_.back()->copy_from(*other.chunk_vec_.back(), last);
    chunk_vec_.back()->construct(*other.chunk_vec_[index], last, CHUNK_SIZE);
  }  // !chunk_vec_.empty()

  // Here other.chunk_vec_.size() > chunk_vec_.size()
  while (chunk_vec_.size() < other.chunk_vec_.size()) {
    size_t src_index = chunk_vec_.size();
    AddChunk();
    size_t last = (chunk_vec_.size() == other.chunk_vec_.size()) ?
                  CHUNK_SIZE - other.last_chunk_left_ : CHUNK_SIZE;
    chunk_vec_.back()->construct(*other.chunk_vec_[src_index], 0, last);
  }
  last_chunk_left_ = other.last_chunk_left_;

  return *this;
}

}  // namespace base
