// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <cstddef>
#include <memory>

namespace base {

// Stupid object pool that pre-allocates N objects (must be default constructable).
// If it's out of preallocated objects - fallbacks to new/delete.
// Does not try to do anything smart. Should be used when we can assume that
// 99% of use-patterns require at most storage_sz_ objects.
// The class is not thread-safe.
template <typename T>
class ObjectPool {
  struct Item {
    T t;
    uint32_t next;
  };

 public:
  explicit ObjectPool(size_t storage_sz)
      : storage_(new Item[storage_sz]), end_(storage_.get() + storage_sz) {
    for (size_t i = 0; i < storage_sz; ++i) {
      storage_[i].next = i + 1;
    }
    available_ = storage_sz;
  }

  // If the object pool is empty returns T from the heap.
  T* Get() {
    if (available_ == 0)
      return new T();

    auto& item = storage_[avail_index_];
    avail_index_ = item.next;
    --available_;

    return &item.t;
  }

  void Release(T* t) {
    if (!IsFrom(t)) {
      delete t;
      return;
    }

    ++available_;
    Item* item = reinterpret_cast<Item*>(t);
    size_t index = item - storage_.get();
    item->next = avail_index_;
    avail_index_ = index;
  }

  bool IsFrom(const T* t) const {
    const Item* item = reinterpret_cast<const Item*>(t);
    return item >= storage_.get() && item < end_;
  }

  size_t available() const {
    return available_;
  }

  // Returns true if the object pool is empty.
  bool empty() const {
    return available_ == 0;
  }

 private:
  std::unique_ptr<Item[]> storage_;
  Item* end_;
  size_t avail_index_ = 0, available_ = 0;
};

template <typename T> class ScopeGuard {
 public:
  ScopeGuard() : pool_(nullptr), obj_(nullptr) {}

  ScopeGuard(ObjectPool<T>* pool) : pool_(pool), obj_(pool->Get()) {
  }
  ScopeGuard(const ScopeGuard&) = delete;

  ScopeGuard(ScopeGuard&& o) noexcept : pool_(o.pool_), obj_(o.obj_) {
    o.obj_ = nullptr;
    o.pool_ = nullptr;
  }

  ~ScopeGuard() {
    if (obj_)
      pool_->Release(obj_);
  }

  void operator=(const ScopeGuard&) = delete;
  ScopeGuard& operator=(ScopeGuard&& o) noexcept {
    if (obj_)
      pool_->Release(obj_);
    obj_ = o.obj_;
    pool_ = o.pool_;
    o.obj_ = nullptr;
    o.pool_ = nullptr;
    return *this;
  }

  T* operator->() { return obj_;}
  T* Get() { return obj_; }

  T* Release() {
    T* res = obj_;
    obj_ = nullptr;
    return res;
  }

  void Swap(ScopeGuard* o) {
    std::swap(pool_, o->pool_);
    std::swap(obj_, o->obj_);
  }
 private:
  ObjectPool<T>* pool_;
  T* obj_;
};

}  // namespace base
