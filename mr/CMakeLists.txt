cxx_proto_lib(mr3)

add_library(mr3_lib mr.cc pipeline.cc pipeline_executor.cc local_runner.cc mr_main.cc)
cxx_link(mr3_lib absl_variant absl_str_format absl_flat_hash_map base mr3_impl_lib
         fiber_file asio_fiber_lib)
add_dependencies(mr3_lib rapidjson_project)

add_subdirectory(impl)

add_library(mr_test_lib test_utils.cc)
cxx_link(mr_test_lib mr3_lib gtest_main)

cxx_test(mr_test mr_test_lib LABELS CI)
