add_executable(fiber_demo fiber_demo.cc)
cxx_link(fiber_demo base TRDP::folly dl)

add_executable(file_read file_read.cc)
cxx_link(file_read base TRDP::folly dl)

add_executable(boost_fibers boost_fibers.cc)
cxx_link(boost_fibers fibers base dl ${Boost_LIBRARIES})


add_executable(mr_demo_fibers mr_demo.cc)
cxx_link(mr_demo_fibers fibers base file dl ${Boost_LIBRARIES})

add_executable(asio asio.cc)
cxx_link(asio fibers base absl_strings ${Boost_LIBRARIES})

