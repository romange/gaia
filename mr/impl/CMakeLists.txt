add_library(mr3_impl_lib dest_file_set.cc)
cxx_link(mr3_impl_lib strings fiber_file mr3_proto)
