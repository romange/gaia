add_library(pprint_utils pprint_utils.cc file_printer.cc)
target_link_libraries(pprint_utils threads strings pb2json proto_writer plang_parser_bison
                      TRDP::protobuf)

add_executable(lst_print_example lst_print_example.cc)
target_link_libraries(lst_print_example pprint_utils)


cxx_test(pprint_utils_test  pprint_utils pprint_utils_test_proto base LABELS CI)

cxx_proto_lib(pprint_utils_test)
