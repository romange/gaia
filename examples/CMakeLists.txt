add_executable(file_read file_read.cc)
cxx_link(file_read base TRDP::folly dl)

add_executable(asio_fibers asio_fibers.cc)
cxx_link(asio_fibers base http_v2 rpc sentry strings)

add_executable(http_client_tool http_client_tool.cc)
cxx_link(http_client_tool http_client_lib asio_fiber_lib)

add_executable(mr3 mr3.cc)
cxx_link(mr3 fiber_file asio_fiber_lib mr3_lib absl_hash absl_str_format http_v2)


add_executable(gsod_group gsod_group.cc)
cxx_link(gsod_group mr3_lib absl_hash absl_str_format http_v2)
