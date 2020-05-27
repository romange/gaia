// Copyright 2020, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include <fcntl.h>
#include <mimalloc.h>
#include <sys/mman.h>  // mmap

#include "base/gtest.h"
#include "base/logging.h"
#include "absl/strings/str_cat.h"

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

    mi_register_mmap_fun(&Mmap, nullptr);
    mi_register_munmap_fun(&Munmap, nullptr);
  }

  static void TearDownTestCase() {
    mi_collect(true);

    LOG(INFO) << "TearDownTestCase";
    mi_stats_print_out(&StatsPrint, nullptr);
  }

  char* MmapShared(string file_name, size_t size, bool is_huge);

  static void* Mmap(void* addr, size_t size, size_t try_alignment, mi_mmap_flags_t mmap_flags,
                    void* arg, bool* is_large);
  static bool Munmap(void* addr, size_t size, void* arg);

  int fd_ = -1;
};

void* MimallocTest::Mmap(void* addr, size_t size, size_t try_alignment, mi_mmap_flags_t mmap_flags,
                         void* arg, bool* is_large) {
  LOG(INFO) << "MMAP S/A/F: " << size << "/" << try_alignment << "/" << mmap_flags;

  int protect_flags = mmap_flags & mi_mmap_commit ? (PROT_WRITE | PROT_READ) : PROT_NONE;
  int flags = MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE;
  *is_large = false;

  void* res = mmap(addr, size, protect_flags, flags, -1, 0);
  if (res == MAP_FAILED)
    res = NULL;

  return res;
}

bool MimallocTest::Munmap(void* addr, size_t size, void* arg) {
  LOG(INFO) << "Munmap A/S: " << addr << "/" << size;
  return munmap(addr, size) == -1;
}

char* MimallocTest::MmapShared(string file_name, size_t size, bool is_huge) {
  string path = absl::StrCat("/dev/", is_huge ? "hugepages" : "shm", "/", file_name);
  fd_ = open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0666);
  if (fd_ == -1)
    return NULL;

  CHECK_GT(fd_, 0);

  int flags = MAP_SHARED;
  if (is_huge)
    flags |= MAP_HUGETLB;

  LOG(INFO) << "Before Mmap";
  char* p1 = reinterpret_cast<char*>(
      mmap(NULL, size, PROT_WRITE | PROT_READ, flags, fd_, 0));
  CHECK_NE(p1, MAP_FAILED);
  LOG(INFO) << "After Mmap";
  CHECK_EQ(0, ftruncate(fd_, size));
  LOG(INFO) << "After ftruncate";
  return p1;
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
}

TEST_F(MimallocTest, HeapLargeAlloc) {
  mi_heap_t* heap = mi_heap_new();
  void* ptr = mi_heap_malloc(heap, 1 << 25);  // 32MB
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

TEST_F(MimallocTest, ShMapUnmap) {
  constexpr size_t kRegionSize = 1 << 30;
  constexpr size_t kUnmapSize = 1 << 22;
  char* p1 = MmapShared("file2.sh", kRegionSize, false);
  memset(p1, 'R', kRegionSize);

  CHECK_EQ(0, munmap(p1, kUnmapSize));

  ASSERT_EQ(0, fallocate(fd_, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, 0, kUnmapSize)) << errno;
  close(fd_);

  CHECK_EQ(0, munmap(p1 + kUnmapSize, kRegionSize - kUnmapSize));
}

TEST_F(MimallocTest, ShMapUnmapHuge) {
  // It seems that THB create significant latency in mmap.
  // TBD: It may be that explicit huge pages won't have this problem.
  constexpr size_t kRegionSize = 1 << 30;
  constexpr size_t kUnmapSize = 1 << 22;
  char* p1 = MmapShared("file2.sh", kRegionSize, true);
  if (p1 == nullptr) {
    LOG(WARNING) << "Omitting - huge pages are not configured";
  }

  memset(p1, 'R', kRegionSize);

  CHECK_EQ(0, munmap(p1, kUnmapSize));

  ASSERT_EQ(0, fallocate(fd_, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, 0, kUnmapSize)) << errno;
  close(fd_);

  CHECK_EQ(0, munmap(p1 + kUnmapSize, kRegionSize - kUnmapSize));
}

}  // namespace util
