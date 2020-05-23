// Copyright 2020, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include <fcntl.h>
#include <mimalloc.h>
#include <sys/mman.h>  // mmap

#include "base/gtest.h"
#include "base/logging.h"

using namespace std;

namespace util {

static void StatsPrint(const char* msg, void* arg) {
  LOG(INFO) << "MI_Stats: " << msg;
}

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
    // mi_option_enable(mi_option_verbose);

    register_mmap_fun(&Mmap, nullptr);
    register_munmap_fun(&Munmap, nullptr);
  }

  static void TearDownTestCase() {
    mi_collect(true);

    LOG(INFO) << "TearDownTestCase";
    mi_stats_print_out(&StatsPrint, nullptr);
  }


  static void* Mmap(void* addr, size_t size, size_t try_alignment, mi_mmap_flags_t mmap_flags,
                    void* arg, bool* is_large);
  static bool Munmap(void* addr, size_t size, void* arg);
};

void* MimallocTest::Mmap(void* addr, size_t size, size_t try_alignment, mi_mmap_flags_t mmap_flags,
                         void* arg, bool* is_large) {
  LOG(INFO) << "MMAP S/A/F: " << size << "/" << try_alignment << "/" << mmap_flags;

  int protect_flags = mmap_flags & mi_mmap_commit ? (PROT_WRITE | PROT_READ) : PROT_NONE;
  int flags = MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE;
  int fd = -1;
  *is_large = false;

  void* res = mmap(addr, size, protect_flags, flags, fd, 0);
  if (res == MAP_FAILED)
    res = NULL;

  return res;
}

bool MimallocTest::Munmap(void* addr, size_t size, void* arg) {
  LOG(INFO) << "Munmap A/S: " << addr << "/" << size;
  return munmap(addr, size) == -1;
}

bool PrintHeap(const mi_heap_t* heap, const mi_heap_area_t* area, void* block, size_t block_size,
               void* arg) {
  LOG(INFO) << "Area(Bs/Comitted/Reserved/Used) " << area->block_size << "/" << area->committed
            << "/" << area->reserved << "/" << area->used << " " << area->blocks << " "
            << block_size;
  return true;
}


TEST_F(MimallocTest, Heap) {
  mi_heap_t* heap = mi_heap_new();
  ASSERT_TRUE(heap != nullptr);
  LOG(INFO) << "Before Allocs";
  mi_stats_print_out(&StatsPrint, nullptr);

  const size_t kBufSize = 64;
  void* ptr;
  for (size_t i = 0; i < 10000; ++i) {
    ptr = mi_heap_malloc_aligned(heap, kBufSize, 16);
  }

  ASSERT_TRUE(mi_heap_visit_blocks(heap, false, &PrintHeap, nullptr));

  for (size_t i = 0; i < 10000; ++i) {
    ptr = mi_heap_malloc_aligned(heap, 32, 16);
  }
  LOG(INFO) << "After 32 bytes allocations";
  ASSERT_TRUE(mi_heap_visit_blocks(heap, false, &PrintHeap, nullptr));

  mi_heap_destroy(heap);
  (void)ptr;
  LOG(INFO) << "After HeapDestroy";
  mi_stats_print_out(&StatsPrint, nullptr);

  heap = mi_heap_new();
  ptr = mi_heap_malloc(heap, 1 << 25);
  memset(ptr, 0, 1 << 25);
  mi_heap_destroy(heap);
}

TEST_F(MimallocTest, MmapPrivateReshared) {
  constexpr size_t kRegionSize = 1 << 20;
  constexpr size_t kItemCnt = kRegionSize / sizeof(uint32_t);

  void* p1 = mmap(0, kRegionSize, PROT_WRITE | PROT_READ,
                  MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
  ASSERT_TRUE(p1 != MAP_FAILED);
  uint32_t* array = reinterpret_cast<uint32_t*>(p1);
  uint32_t val = 0xdeadbeaf;

  std::fill(array, array + kItemCnt, val);
  string map_file = "/dev/shm/mmap.file";

  int fd = open(map_file.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0666);
  CHECK_GT(fd, 0);
  for (size_t i = 0; i < kItemCnt; ++i) {
    ASSERT_EQ(val, array[i]) << i;
  }

  void* p2 = mmap(p1, kRegionSize, PROT_WRITE | PROT_READ, MAP_SHARED | MAP_FIXED, fd, 0);
  CHECK_EQ(0, ftruncate(fd, kRegionSize));
  ASSERT_TRUE(p2 == p1);
  for (size_t i = 0; i < kItemCnt; ++i) {
    benchmark::DoNotOptimize(array[i]);
  }
  for (size_t i = 0; i < kItemCnt; ++i) {
    ASSERT_EQ(0, array[i]);
  }
  close(fd);
}

}  // namespace util
