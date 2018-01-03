add_executable(fiber_demo fiber_demo.cc)
cxx_link(fiber_demo base TRDP::folly dl)

add_executable(file_read file_read.cc)
cxx_link(file_read base TRDP::folly dl)

add_executable(boost_fibers boost_fibers.cc)
cxx_link(boost_fibers fibers base dl ${Boost_LIBRARIES})
