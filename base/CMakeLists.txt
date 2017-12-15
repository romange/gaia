add_library(base bits.cc init.cc logging.cc walltime.cc pthread_utils.cc)
cxx_link(base TRDP::glog TRDP::gflags atomic)

cxx_test(bits_test LABELS CI)


# Define default gtest_main for tests.
add_library(gtest_main gtest_main.cc)
target_link_libraries(gtest_main TRDP::glog TRDP::gflags TRDP::gtest base TRDP::benchmark)

