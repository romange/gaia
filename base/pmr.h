// Copyright 2016, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <sparsehash/dense_hash_map>
#include <sparsehash/dense_hash_set>
#include <pmr/polymorphic_allocator.h>

namespace base {

template <typename K,
           typename V,
           typename Hash = SPARSEHASH_HASH<K>,
           typename EqualKey = std::equal_to<K>>
    using pmr_dense_hash_map =
    ::google::dense_hash_map<K, V, Hash, EqualKey,
                             ::pmr::polymorphic_allocator<std::pair<const K, V>>>;

template <typename K,
           typename Hash = SPARSEHASH_HASH<K>,
           typename EqualKey = std::equal_to<K>>
    using pmr_dense_hash_set =
    ::google::dense_hash_set<K, Hash, EqualKey,
                             ::pmr::polymorphic_allocator<K>>;


template <typename T> class PmrDeleterBase {
 public:
  typedef typename ::pmr::polymorphic_allocator<T>::pointer pointer;
  typedef ::pmr::polymorphic_allocator<T> allocator_type;

  static_assert(sizeof(T) >= 0, "T must be well defined");

  PmrDeleterBase() {}
  PmrDeleterBase(const allocator_type& alloc) : alloc_(alloc) {}
 protected:
  allocator_type alloc_;
};

template <typename T> class PmrDeleter : public PmrDeleterBase<T> {
public:
  using typename PmrDeleterBase<T>::allocator_type;

  PmrDeleter(const allocator_type& alloc) : PmrDeleterBase<T>(alloc) {}

  void operator()(typename PmrDeleterBase<T>::pointer p) {
    std::allocator_traits<allocator_type>::destroy(this->alloc_, p);
    std::allocator_traits<allocator_type>::deallocate(this->alloc_, p, 1);
  }
};

template <typename T> class PmrDeleter<T[]> : public PmrDeleterBase<T> {
public:
  using typename PmrDeleterBase<T>::allocator_type;
  using typename PmrDeleterBase<T>::pointer;

  PmrDeleter() : sz_(0) {}
  PmrDeleter(const allocator_type& alloc, size_t sz) : PmrDeleterBase<T>(alloc), sz_(sz) {}

  void operator()(pointer p) {
    for (size_t i = 0; i < sz_; ++i) {
      std::allocator_traits<allocator_type>::destroy(this->alloc_, p + i);
    }
    std::allocator_traits<allocator_type>::deallocate(this->alloc_, p, sz_);
  }

 private:
  size_t sz_;
};


template <typename T> using pmr_unique_ptr = std::unique_ptr<T, PmrDeleter<T>>;

template<typename T, typename ...Args>
    pmr_unique_ptr<T> make_pmr_unique(::pmr::memory_resource* mr, Args&&... args) {
  ::pmr::polymorphic_allocator<T> alloc(mr);

  T* ptr = alloc.allocate(1);
  std::allocator_traits<decltype(alloc)>::construct(alloc, ptr, std::forward<Args>(args)...);

  return pmr_unique_ptr<T>(ptr, alloc);
}

namespace detail {

template<typename Allocator, typename ...Args> void DoConstruct(std::false_type , Allocator& alloc,
  typename Allocator::pointer p, size_t sz, Args&&... args) {
  using atraits = std::allocator_traits<Allocator>;

  for (size_t i = 0; i < sz; ++i) {
    atraits::construct(alloc, p + i, std::forward<Args>(args)...);
  }
}

template<typename Allocator, typename ...Args> void DoConstruct(std::true_type , Allocator& alloc,
  typename Allocator::pointer p, size_t sz, Args&&... args) {
  using value_type = typename Allocator::value_type;

  std::fill(p, p + sz, value_type{std::forward<Args>(args)...});
}

}  // namespace detail

template<typename T, typename ...Args>
    pmr_unique_ptr<T[]> make_pmr_array(::pmr::memory_resource* mr, size_t sz, Args&&... args) {
  ::pmr::polymorphic_allocator<T> alloc(mr);

  T* ptr = alloc.allocate(sz);
  detail::DoConstruct(typename std::is_trivial<T>::type{}, alloc, ptr, sz,
                      std::forward<Args>(args)...);

  return pmr_unique_ptr<T[]>(ptr, PmrDeleter<T[]>(alloc, sz));
}



}  // namespace base
