add_library(asio_fiber_lib io_context_pool.cc connection.cc)
cxx_link(asio_fiber_lib base http ${Boost_LIBRARIES})
