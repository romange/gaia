add_library(rpc frame_format.cc)
cxx_link(rpc base status strings ${Boost_LIBRARIES})
