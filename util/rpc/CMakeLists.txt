add_library(rpc frame_format.cc rpc_server.cc)
cxx_link(rpc base asio_fiber_lib status strings ${Boost_LIBRARIES})

cxx_test(rpc_server_test rpc LABELS CI)
