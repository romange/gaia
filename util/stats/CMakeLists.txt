add_library(stats_lib sliding_counter.cc)
cxx_link(stats_lib base)
cxx_test(sliding_counter_test stats_lib)