add_library(pprint_utils pprint_utils.cc file_printer.cc)
target_link_libraries(pprint_utils threads strings pb2json TRDP::protobuf)

add_executable(pprint pprint.cc)
target_link_libraries(pprint plang_parser_bison pprint_utils proto_writer file)


cxx_test(pprint_utils_test  pprint_utils pprint_utils_test_proto base LABELS CI)

cxx_proto_lib(pprint_utils_test)
