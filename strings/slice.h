// Copyright 2013, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#ifndef SLICE_H
#define SLICE_H

#include <algorithm>
#include <cstring>
#include <iterator>
#include <cassert>
#include <string>

#include "base/integral_types.h"

namespace strings {
template<typename T> class SliceBase {
 protected:
  const T* ptr_;
  uint32 length_;

 public:
  SliceBase() : ptr_(NULL), length_(0) {}

  SliceBase(const T* offset, size_t len): ptr_(offset), length_(len) {
  }

  // Substring of another SliceBase.
  // pos must be non-negative and <= x.length().
  SliceBase(SliceBase x, size_t pos) : ptr_(x.ptr_ + pos), length_(x.length_ - pos)  {
    assert(pos < x.length_);
  }

  // Substring of another SliceBase.
  // pos must be <= x.length().
  // len will be pinned to at most x.length() - pos.
  SliceBase(SliceBase x, size_t pos, size_t len)
    :ptr_(x.ptr_ + pos), length_(std::min(len, x.length_ - pos)) {
    assert(pos <= x.length_);
  }

  // data() may return a pointer to a buffer with embedded NULs, and the
  // returned buffer may or may not be null terminated.  Therefore it is
  // typically a mistake to pass data() to a routine that expects a NUL
  // terminated string.
  const T* data() const { return ptr_; }
  size_t size() const { return length_; }
  size_t length() const { return length_; }
  bool empty() const { return length_ == 0; }

  void clear() {
    ptr_ = nullptr;
    length_ = 0;
  }

  void set(const T* data_arg, size_t len) {
    ptr_ = data_arg;
    length_ = len;
  }

  void set(const T* start, const T* end) {
    ptr_ = start;
    length_ = end - start;
  }

  const T& operator[](size_t i) const {
    assert(i < length_);
    return ptr_[i];
  }

  void remove_prefix(size_t n) {
    assert(length_ >= n);
    ptr_ += n;
    length_ -= n;
  }

  void remove_suffix(size_t n) {
    assert(length_ >= n);
    length_ -= n;
  }

  void truncate(size_t n) {
    assert(length_ >= n);
    length_ = n;
  }

  bool equals(SliceBase x) const {
    return x.size() == size() && memcmp(ptr_, x.ptr_, size()) == 0;
  }

  // returns {-1, 0, 1}
  int compare(SliceBase x) const {
    const size_type min_size = std::min(length_,x.length_);
    int r = memcmp(ptr_, x.ptr_, min_size);
    if (r < 0) return -1;
    if (r > 0) return 1;
    if (length_ < x.length_) return -1;
    if (length_ > x.length_) return 1;
    return 0;
  }

  bool starts_with(SliceBase x) const {
    return (length_ >= x.length_) && (memcmp(ptr_, x.ptr_, x.length_) == 0);
  }

  bool ends_with(SliceBase x) const {
    return ((length_ >= x.length_) &&
            (memcmp(ptr_ + (length_-x.length_), x.ptr_, x.length_) == 0));
  }

  // standard STL container boilerplate
  typedef T value_type;
  typedef const T* pointer;
  typedef const T& reference;
  typedef const T& const_reference;
  typedef size_t size_type;
  typedef std::ptrdiff_t difference_type;

  static const size_type npos = size_type(-1);

  typedef const T* const_iterator;
  typedef const T* iterator;
  typedef std::reverse_iterator<const_iterator> const_reverse_iterator;
  typedef std::reverse_iterator<iterator> reverse_iterator;
  iterator begin() const { return ptr_; }
  iterator end() const { return ptr_ + length_; }
  const_reverse_iterator rbegin() const {
    return const_reverse_iterator(ptr_ + length_);
  }
  const_reverse_iterator rend() const {
    return const_reverse_iterator(ptr_);
  }
  size_type max_size() const { return length_; }
  size_type capacity() const { return length_; }

  // cpplint.py emits a false positive [build/include_what_you_use]
  size_type copy(T* buf, size_type n, size_type pos = 0) const;  // NOLINT

  bool contains(SliceBase s) const;

  size_type find(T c, size_type pos = 0) const {
    if (length_ == 0 || pos >= length_) {
       return npos;
    }
    size_type i = std::find(ptr_ + pos, ptr_ + length_, c) - ptr_;
    return i == length_ ? npos : i;
  }

  size_type find(SliceBase s, size_type pos = 0) const {
    if (s.length() + pos > length())
      return npos;
    size_type i = std::search(begin() + pos, end(), s.begin(), s.end()) - begin();
    return i == length_ ? npos : i;
  }

  size_type rfind(T c, size_type pos = npos) const;
  size_type rfind(SliceBase s, size_type pos) const;
  /*size_type find_first_of(T c, size_type pos = 0) const {
    return find(c, pos);
  }*/
  // size_type find_first_not_of(T c, size_type pos = 0) const;
  size_type find_last_not_of(T c, size_type pos = npos) const;
} __attribute__((packed));

template<typename T> size_t SliceBase<T>::rfind(SliceBase s, size_type pos) const {
  if (length_ < s.length_) return npos;
  const size_t ulen = length_;
  if (s.length_ == 0) return std::min(ulen, pos);

  const char* last = ptr_ + std::min(ulen - s.length_, pos) + s.length_;
  const char* result = std::find_end(ptr_, last, s.ptr_, s.ptr_ + s.length_);
  return result != last ? result - ptr_ : npos;
}

// Search range is [0..pos] inclusive.  If pos == npos, search everything.
template<typename T> size_t SliceBase<T>::rfind(T c, size_type pos) const {
  // Note: memrchr() is not available on Windows.
  if (length_ <= 0) return npos;
  for (int i =
      std::min(pos, static_cast<size_type>(length_ - 1));
       i >= 0; --i) {
    if (ptr_[i] == c) {
      return i;
    }
  }
  return npos;
}

template<typename T> size_t SliceBase<T>::find_last_not_of(T c, size_type pos) const {
  if (length_ <= 0) return npos;

  for (difference_type i = std::min(pos, size_t(length_ - 1)); i >= 0; --i) {
    if (ptr_[i] != c) {
      return i;
    }
  }
  return npos;
}

template<typename T> bool operator==(const SliceBase<T>& a, const SliceBase<T>& b) {
  return a.equals(b);
}

#if 0
class Slice : public SliceBase<unsigned char> {
  typedef SliceBase<unsigned char> Base;
public:
  // C'tor inheritance not yet implemented.
  Slice() : Base() {}

  Slice(const unsigned char* offset, size_t len): Base(offset, len) {
  }

  // If this c'tor does not exist, then if we pass const char* then string c'tor is used that
  // converts the first argument to string and the second one to pos.
  // It's clearly causes bugs: binary strings treated wrong and the second argument has
  // different semantics.
  Slice(const char* ptr, size_t len): Base(reinterpret_cast<const unsigned char*>(ptr), len) {
  }

  // Substring of another Slice.
  // pos must be non-negative and <= x.length().
  Slice(Base x, size_t pos) : Base(x, pos)  {
  }

  // Substring of another Slice.
  // pos must be <= x.length().
  // len will be pinned to at most x.length() - pos.
  Slice(Base x, size_t pos, size_t len) : Base(x, pos, len) {
  }

  Slice(const std::string& s, size_t pos = 0)
     : Base(reinterpret_cast<const unsigned char*>(s.data()) + pos, s.size() - pos) {
    assert(pos <= s.size());
  }

  std::string as_string() const {
    return std::string(charptr(), length_);
  }

  const char* charptr() const {
    return ::strings::charptr(ptr_);
  }

  static Slice FromCstr(const char* cstr) { return Slice(cstr, strlen(cstr)); }
};

#endif
}  // namespace strings

#endif  // SLICE_H
