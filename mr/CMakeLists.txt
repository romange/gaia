cxx_proto_lib(mr3)

add_library(mr3_lib mr.cc pipeline.cc joiner_executor.cc local_runner.cc
            mapper_executor.cc mr_pb.cc mr_main.cc)
cxx_link(mr3_lib absl_flat_hash_map absl_variant absl_str_format base mr3_impl_lib
         fiber_file asio_fiber_lib gce_lib pb2json TRDP::rapidjson)
add_subdirectory(impl)

add_library(mr_test_lib test_utils.cc)
cxx_link(mr_test_lib mr3_lib absl_flat_hash_map gtest_main)

cxx_test(mr_test mr_test_lib addressbook_proto LABELS CI)
cxx_test(local_runner_test mr_test_lib addressbook_proto LABELS CI)
