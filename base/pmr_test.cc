// Copyright 2016, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "base/pmr.h"

#include "base/gtest.h"

namespace base {

class PmrTest { };

class test_memory_resource : public pmr::memory_resource {
  pmr::memory_resource* upstream_;
 public:
  test_memory_resource(pmr::memory_resource* upstream = pmr::get_default_resource())
      : upstream_(upstream) {}

  void* do_allocate(std::size_t bytes,
                 std::size_t align = alignof(std::max_align_t)) override {
    allocated_bytes += bytes;
    return upstream_->allocate(bytes, align);
  }

  void do_deallocate(void* ptr, std::size_t bytes,
                     std::size_t align = alignof(std::max_align_t)) override {
    allocated_bytes -= bytes;
    return upstream_->deallocate(ptr, bytes, align);
  }

  bool do_is_equal(const memory_resource& other) const override {
    return &other == this;
  }

  ssize_t allocated_bytes = 0;
};

class Unimplemented;

TEST(PmrTest, Base) {
  test_memory_resource tmr;
  pmr::polymorphic_allocator<int> alloc(&tmr);

  {
    pmr_unique_ptr<int> foo(alloc.allocate(1), alloc);
  }
  EXPECT_EQ(0, tmr.allocated_bytes);

  {
    pmr_unique_ptr<int[]> foo(alloc.allocate(2), PmrDeleter<int[]>(alloc, 2));
  }
  EXPECT_EQ(0, tmr.allocated_bytes);

  using FooType = pmr_unique_ptr<Unimplemented>;
  (void)((FooType*)nullptr);
  // FooType ftype;
}


TEST(PmrTest, Make) {
  test_memory_resource tmr;
  {
    pmr_unique_ptr<int> foo = make_pmr_unique<int>(&tmr, 5);
    EXPECT_EQ(5, *foo);
  }
  EXPECT_EQ(0, tmr.allocated_bytes);

  {
    pmr_unique_ptr<int[]> foo = make_pmr_array<int>(&tmr, 20, 5);
    for (size_t i = 0; i < 20; ++i) {
      EXPECT_EQ(5, foo[i]);
    }
  }
  EXPECT_EQ(0, tmr.allocated_bytes);
}

class NonCopyable {
public:
  NonCopyable(int i) {}
  NonCopyable(const NonCopyable&) = delete;
  void operator=(const NonCopyable&) = delete;
};

static_assert(!std::is_trivial<NonCopyable>::value, "");

TEST(PmrTest, Array) {
  test_memory_resource tmr;
  pmr_unique_ptr<NonCopyable[]> foo = make_pmr_array<NonCopyable>(&tmr, 20, 5);
}

}  // namespace base
