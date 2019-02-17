cxx_proto_lib(mr3)

add_library(mr3_lib mr.cc)
cxx_link(mr3_lib base mr3_proto)
