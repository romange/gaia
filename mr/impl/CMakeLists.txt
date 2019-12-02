add_library(mr3_impl_lib local_context.cc dest_file_set.cc freq_map_wrapper.cc)
cxx_link(mr3_impl_lib strings fiber_file proto_writer mr3_proto)
