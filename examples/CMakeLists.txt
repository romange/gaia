add_executable(fiber_demo fiber_demo.cc)
cxx_link(fiber_demo base TRDP::folly dl)
