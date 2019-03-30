add_library(file file.cc file_util.cc filesource.cc gzip_file.cc list_file.cc list_file_reader.cc
            meta_map_block.cc compressors.cc lst2_impl.cc)
cxx_link(file base strings util TRDP::lz4 TRDP::crc32c)

add_library(test_util test_util.cc)
target_link_libraries(test_util base file gtest_main)

cxx_proto_lib(proto_writer_test DEPENDS addressbook_proto)

add_library(proto_writer proto_writer.cc)
cxx_link(proto_writer file TRDP::protobuf util)

add_library(lz4_file lz4_file.cc)
cxx_link(lz4_file file TRDP::lz4)

add_library(list_file_py SHARED list_file_py.cc)
set_target_properties(list_file_py PROPERTIES PREFIX "")

target_link_libraries(list_file_py python2.7 file)
add_include(list_file_py "/usr/include/python2.7/")

add_library(fiber_file fiber_file.cc)
cxx_link(fiber_file file fibers_ext)


cxx_test(file_test file lz4_file LABELS CI)
cxx_test(list_file_test file test_util LABELS CI)
cxx_test(proto_writer_test proto_writer proto_writer_test_proto LABELS CI)

