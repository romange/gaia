cxx_proto_lib(mr3)

add_library(mr3_lib mr.cc pipeline.cc pipeline_executor.cc local_runner.cc)
cxx_link(mr3_lib absl_variant base mr3_proto fiber_file asio_fiber_lib)

cxx_test(mr_test mr3_lib LABELS CI)
