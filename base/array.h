// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include <array>
#include <type_traits>

namespace base {

template <typename T, std::size_t N>
class array {
    static_assert(N > 0, "");
 public:
    typedef T value_type;
    typedef std::size_t size_type;
    typedef std::ptrdiff_t difference_type;
    typedef value_type& reference;
    typedef const value_type& const_reference;
    typedef value_type* pointer;
    typedef const value_type* const_pointer;
    typedef pointer iterator;
    typedef const_pointer const_iterator;
    typedef std::reverse_iterator<iterator> reverse_iterator;
    typedef std::reverse_iterator<const_iterator> const_reverse_iterator;


    array(const T& t = T()) {
      for (auto& v : array_)
        new (&v) T{t};
    }

    ~array() {
      for (auto it = begin(); it != end(); ++it)
        it->~T();
    }

    template <typename... Args> array(Args&&... args) {
      for (auto& v : array_)
        new (&v) T(std::forward<Args>(args)...);
    }


    array& operator=(const array& a) {
      for (size_t i = 0; i < N; ++i) {
        this->operator[](i) = a[i];
      }
      return *this;
    }

    array(const array& a) {
      for (size_t i = 0; i < N; ++i) {
        new (&array_[i]) T{a[i]};
      }
    }

    reference operator[](std::size_t i) { return *reinterpret_cast<pointer>(&array_[i]);}

    const_reference operator[](std::size_t i) const {
     return *reinterpret_cast<const_pointer>(&array_[i]);
    }

    iterator begin() { return reinterpret_cast<pointer>(array_.begin()); }
    iterator end() { return reinterpret_cast<pointer>(array_.end()); }
    const_iterator begin() const { return reinterpret_cast<const_pointer>(array_.begin()); }
    const_iterator end() const { return reinterpret_cast<const_pointer>(array_.end()); }

    constexpr static std::size_t size() { return N;}
 private:
    typedef typename std::aligned_storage<sizeof(value_type),
                                          std::alignment_of<value_type>::value>::type storage_type;

    typedef std::array<storage_type, N> ArrayType;

    ArrayType array_;
};

}  // namespace base
