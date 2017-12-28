cxx_proto_lib(addressbook)
cxx_test(proto_test addressbook_proto)

add_library(plang plang.cc)
target_link_libraries(plang protobuf math plang_parser_bison)

flex_lib(plang_scanner)

bison_lib(plang_parser)
target_link_libraries(plang_parser_bison strings plang plang_scanner_flex glog)

cxx_test(plang_test plang plang_parser_bison addressbook_proto)