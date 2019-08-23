add_library(asio_fiber_lib io_context.cc io_context_pool.cc
            connection_handler.cc yield.cc accept_server.cc periodic_task.cc
            glog_asio_sink.cc fiber_socket.cc prebuilt_asio.cc)
cxx_link(asio_fiber_lib base stats_lib fibers_ext absl_optional)

add_definitions(-DBOOST_ASIO_NO_DEPRECATED)

cxx_test(periodic_task_test asio_fiber_lib LABELS CI)
cxx_test(io_context_test asio_fiber_lib LABELS CI)
cxx_test(fiber_socket_test http_test_lib LABELS CI)
