// Copyright 2020, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "base/gtest.h"

#include <mimalloc.h>

#include "base/logging.h"

using namespace std;

namespace util {

class MimallocTest : public testing::Test {
 protected:
  MimallocTest() {
  }

  void SetUp() override {
  }

  void TearDown() {
  }

  static void SetUpTestCase() {
    mi_process_init();
  }
};

bool PrintHeap(const mi_heap_t* heap, const mi_heap_area_t* area, void* block, size_t block_size, void* arg) {
  LOG(INFO) << "Area(Bs/Comitted/Reserved/Used) " << area->block_size << "/" << area->committed
            << "/" << area->reserved << "/" << area->used << " " << area->blocks
            << " " << block_size;
  return true;
}

TEST_F(MimallocTest, Heap) {
  mi_heap_t* heap = mi_heap_new();
  const size_t kBufSize = 64;
  for (size_t i = 0; i < 10000; ++i) {
    void* ptr = mi_heap_malloc_aligned(heap, kBufSize, 16);
    memset(ptr, 0, kBufSize);
  }
  ASSERT_TRUE(mi_heap_visit_blocks(heap, false, &PrintHeap, nullptr));
  mi_stats_print(nullptr);

  mi_heap_destroy(heap);
}

}  // namespace util
