// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <memory>

namespace base {

// C++ new/delete can not allocate arrays initialized with c'tor.
// For that we use allocate_unique_array:
// my_unique_arr arr = allocate_unique_array(count, alloc)

template <typename Alloc> class AllocArrayDeleter {
  Alloc alloc_;
  size_t sz_;
public:
  AllocArrayDeleter(Alloc alloc, size_t s) : alloc_(alloc), sz_(s) {}

  void operator()(typename std::allocator_traits<Alloc>::pointer p) {
    for (size_t i = 0; i < sz_; ++i) {
      std::allocator_traits<Alloc>::destroy(alloc_, p + i);
    }
    std::allocator_traits<Alloc>::deallocate(alloc_, p, sz_);
  }
};

template<typename Alloc, typename... Args>
    std::unique_ptr<typename Alloc::value_type[], AllocArrayDeleter<Alloc>>
allocate_unique_array(size_t count, Alloc alloc, Args&&... args) {
  std::unique_ptr<typename Alloc::value_type[], AllocArrayDeleter<Alloc>> res{
        alloc.allocate(count), AllocArrayDeleter<Alloc>(alloc, count)};
  for (size_t i = 0; i < count; ++i) {
    std::allocator_traits<Alloc>::construct(alloc, res.get() + i, std::forward<Args>(args)...);
  }
  return res;
}


}  // namespace base
