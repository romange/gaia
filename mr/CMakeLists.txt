cxx_proto_lib(mr3)

add_library(mr3_lib mr.cc mr_executor.cc)
cxx_link(mr3_lib absl_variant base mr3_proto fiber_file)
