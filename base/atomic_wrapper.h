// Copyright 2014, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#ifndef _BASE_ATOMIC_WRAPPER_H
#define _BASE_ATOMIC_WRAPPER_H

#include <atomic>

// This class helps with the problem when we want to store
// atomic numbers inside container.
// In this case we want to create them first and then change them.
// Unortunately most containers do not allow this because they require
// copy c'tor and assignment operator. This wrapper solves this by providing
// assignment semantics.
namespace base {

template <typename T> struct atomic_wrapper {
  std::atomic<T> a_;

public:
  atomic_wrapper() : a_(0) {}

  atomic_wrapper(const std::atomic<T>& a) :a_(a.load()) {}

  atomic_wrapper(const atomic_wrapper &other) :a_(other.a_.load())  {}
  explicit atomic_wrapper(const T& a) :a_(a) {}

  atomic_wrapper& operator=(const atomic_wrapper &other) {
    a_.store(other.a_.load());
    return *this;
  }

  std::atomic<T>& atomic() { return a_; }

  operator T () const { return a_.load(); }

  T exchange(T desired, std::memory_order order = std::memory_order_seq_cst ) {
    return a_.exchange(desired, order);
  }

  void store(T t, std::memory_order order) { a_.store(t, order); }

  T load(std::memory_order order) const { return a_.load(order); }

  bool compare_exchange_strong(T& expected, T desired, std::memory_order order) {
    return a_.compare_exchange_strong(expected, desired, order);
  }

  bool compare_exchange_weak(T& expected, T desired, std::memory_order order) {
    return a_.compare_exchange_weak(expected, desired, order);
  }

  T fetch_add(T arg, std::memory_order order) {
    return a_.fetch_add(arg, order);
  }

  T fetch_sub(T arg, std::memory_order order) {
    return a_.fetch_sub(arg, order);
  }

  void AtomicInc() {
    a_.fetch_add(1, std::memory_order_relaxed);
  }
};

}  // namespace base

#endif  // _BASE_ATOMIC_WRAPPER_H