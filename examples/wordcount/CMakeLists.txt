add_executable(warc_parse warc_parse.cc)
cxx_link(warc_parse mr3_lib http_v2 TRDP::re2)

add_executable(word_count word_count.cc)
cxx_link(word_count mr3_lib http_v2 TRDP::re2 TRDP::hyperscan)
