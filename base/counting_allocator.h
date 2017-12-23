#ifndef _BASE_COUNTING_ALLOCATOR_H
#define _BASE_COUNTING_ALLOCATOR_H

#include <cstddef>
#include <memory>
#include <new>

namespace base {
namespace detail {
    auto constexpr header_size = sizeof(std::size_t);
}

template<typename T>
class counting_allocator {
public:
  using pointer = T*;
  using const_pointer = T const*;
  using void_pointer = void*;
  using const_void_pointer = void const*;
  using value_type = T;
  using size_type = std::size_t;
  using difference_type = std::ptrdiff_t;
  typedef T& reference;
  typedef const T& const_reference;

  template<typename U>
  struct rebind {
      using other = counting_allocator<U>;
  };

  counting_allocator()
      : allocated_bytes{std::make_shared<std::size_t>(0)} {
  }

  template<typename U> counting_allocator(const counting_allocator<U>& other)
      : allocated_bytes(other.allocated_bytes) {
  }

  pointer allocate(size_type n, const_void_pointer = 0) {
      pointer ptr = static_cast<pointer>(::operator new(n * sizeof(T)));
      *allocated_bytes += n * sizeof(T) + detail::header_size;
      return ptr;
  }

  void deallocate(pointer ptr, size_type n) {
      ::operator delete(static_cast<void*>(ptr));
      *allocated_bytes -= n * sizeof(T) + detail::header_size;
  }

  std::size_t bytes_allocated() const noexcept {
      return *allocated_bytes;
  }

  size_type max_size() const  {
      return static_cast<size_type>(-1) / sizeof(value_type);
  }

  template <typename U, typename... Args> void construct (U* p, Args&&... args) {
    ::new ((void*)p) U(std::forward<Args>(args)...);
  }

  void destroy(pointer p) { p->~value_type(); }

  template<typename U> bool operator==(const counting_allocator<U>& b) const {
    return allocated_bytes == b.allocated_bytes;
  }
private:
  template <typename U> friend class counting_allocator;
  // Meh.
  std::shared_ptr<std::size_t> allocated_bytes;
};

template<typename T>
bool operator!=(counting_allocator<T> const& a, counting_allocator<T> const& b) {
    return !(a == b);
}
}  // namespace base

#endif // _BASE_COUNTING_ALLOCATOR_H

