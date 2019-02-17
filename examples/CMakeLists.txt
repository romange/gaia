add_executable(folly_fiber folly_fiber.cc)
cxx_link(folly_fiber base TRDP::folly dl)

add_executable(file_read file_read.cc)
cxx_link(file_read base TRDP::folly dl)

add_executable(boost_fibers boost_fibers.cc)
cxx_link(boost_fibers fibers base dl ${Boost_LIBRARIES})

add_executable(asio_co asio_co.cc)
cxx_link(asio_co fibers base absl_strings ${Boost_LIBRARIES})
target_compile_definitions(asio_co PRIVATE BOOST_COROUTINES_NO_DEPRECATION_WARNING)

add_executable(asio_fibers asio_fibers.cc)
cxx_link(asio_fibers base http_v2 rpc sentry strings)

add_executable(http_client_tool http_client_tool.cc)
cxx_link(http_client_tool http_client_lib asio_fiber_lib)

add_executable(mr_demo mr_demo.cc)
cxx_link(mr_demo fiber_file base asio_fiber_lib)

add_executable(mr3 mr3.cc)
cxx_link(mr3 fiber_file asio_fiber_lib mr3_lib)

