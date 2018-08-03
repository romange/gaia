add_executable(pprint pprint.cc)
target_link_libraries(pprint plang_parser_bison threads pprint_utils proto_writer file pb2json)

add_library(pprint_utils pprint_utils.cc)
target_link_libraries(pprint_utils strings TRDP::protobuf)

cxx_test(pprint_utils_test  pprint_utils pprint_utils_test_proto base LABELS CI)

cxx_proto_lib(pprint_utils_test)
