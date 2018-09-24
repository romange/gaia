add_library(asio_fiber_lib io_context_pool.cc client_channel.cc connection_handler.cc yield.cc
            accept_server.cc periodic_task.cc)
cxx_link(asio_fiber_lib base stats_lib ${Boost_LIBRARIES})

cxx_test(periodic_task_test asio_fiber_lib LABELS CI)
cxx_test(io_context_test asio_fiber_lib LABELS CI)
