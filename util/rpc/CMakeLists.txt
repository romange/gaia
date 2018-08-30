add_library(rpc frame_format.cc rpc_server.cc async_client.cc)
cxx_link(rpc base asio_fiber_lib status strings ${Boost_LIBRARIES})

add_library(rpc_test_lib rpc_test_utils.cc)
cxx_link(rpc_test_lib rpc gtest_main)

cxx_test(rpc_server_test rpc_test_lib LABELS CI)
