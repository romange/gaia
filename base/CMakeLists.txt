add_library(base arena.cc bits.cc crc32c.cc coder.cc hash.cc init.cc logging.cc simd.cc varint.cc walltime.cc pthread_utils.cc)
cxx_link(base TRDP::glog TRDP::gflags TRDP::pmr TRDP::xxhash atomic rt)  # rt for timer_create etc.
add_dependencies(base sparsehash_project)

cxx_test(bits_test LABELS CI)
cxx_test(pod_array_test base LABELS CI)
cxx_test(arena_test base strings LABELS CI)
cxx_test(pmr_test base TRDP::pmr LABELS CI)
cxx_test(simd_test base LABELS CI)
cxx_test(crc32c_test base LABELS CI)
cxx_test(walltime_test base LABELS CI)
cxx_test(flit_test base LABELS CI)
cxx_test(cxx_test base LABELS CI)
cxx_test(hash_test base file DATA testdata/ids.txt.gz LABELS CI)
cxx_test(RWSpinLock_test base LABELS CI)
cxx_test(event_count_test base LABELS CI)

# Define default gtest_main for tests.
add_library(gtest_main gtest_main.cc)
target_link_libraries(gtest_main TRDP::glog TRDP::gflags TRDP::gtest base TRDP::benchmark)

