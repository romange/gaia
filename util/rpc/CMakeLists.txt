add_library(rpc frame_format.cc rpc_server.cc)
cxx_link(rpc base status strings ${Boost_LIBRARIES})
