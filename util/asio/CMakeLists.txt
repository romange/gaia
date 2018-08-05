add_library(asio_fiber_lib io_context_pool.cc)
cxx_link(asio_fiber_lib base ${Boost_LIBRARIES})
