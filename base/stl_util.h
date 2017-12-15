// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include <algorithm>

#include <ostream>
#include <vector>

#include "base/integral_types.h"

#if 0
// This is a wrapper for an STL allocator which keeps a count of the
// active bytes allocated by this class of allocators.  This is NOT
// THREAD SAFE.  This should only be used in situations where you can
// ensure that only a single thread performs allocation and
// deallocation.
template <typename T, typename Alloc = std::allocator<T> >
class STLCountingAllocator : public Alloc {
 public:
  typedef typename Alloc::pointer pointer;
  typedef typename Alloc::size_type size_type;

  STLCountingAllocator() : bytes_used_(NULL) { }
  STLCountingAllocator(int64* b) : bytes_used_(b) {}

  // Constructor used for rebinding
  template <class U>
  STLCountingAllocator(const STLCountingAllocator<U>& x)
      : Alloc(x),
        bytes_used_(x.bytes_used()) {
  }

  pointer allocate(size_type n, std::allocator<void>::const_pointer hint = 0) {
    assert(bytes_used_ != NULL);
    *bytes_used_ += n * sizeof(T);
    return Alloc::allocate(n, hint);
  }

  void deallocate(pointer p, size_type n) {
    Alloc::deallocate(p, n);
    assert(bytes_used_ != NULL);
    *bytes_used_ -= n * sizeof(T);
  }

  // Rebind allows an allocator<T> to be used for a different type
  template <class U> struct rebind {
    typedef STLCountingAllocator<U,
                                 typename Alloc::template
                                 rebind<U>::other> other;
  };

  int64* bytes_used() const { return bytes_used_; }

 private:
  int64* bytes_used_;
};

#endif

template<typename T> std::ostream& operator<<(std::ostream& o, const std::vector<T>& vec) {
  o << "[";
  for (size_t i = 0; i < vec.size(); ++i) {
    o << vec[i];
    if (i + 1 < vec.size()) o << ",";
  }
  o << "]";
  return o;
}

namespace base {

template<typename T, typename U> bool _in(const T& t,
    std::initializer_list<U> l) {
  return std::find(l.begin(), l.end(), t) != l.end();
}


template<typename T> struct MinMax {

  T min_val = std::numeric_limits<T>::max(), max_val = std::numeric_limits<T>::min();

  void Update(T val) {
    min_val = std::min(min_val, val);
    max_val = std::max(max_val, val);
  }
};

}  // namespace base
