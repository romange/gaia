add_library(stats_lib sliding_counter.cc varz_stats.cc)
cxx_link(stats_lib strings)
cxx_test(sliding_counter_test stats_lib)
