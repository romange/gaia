add_library(uring_fiber_lib proactor.cc)
cxx_link(uring_fiber_lib base -luring)
