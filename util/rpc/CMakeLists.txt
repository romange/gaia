add_library(rpc frame_format.cc rpc_connection.cc channel.cc service_descriptor.cc
            impl/rpc_conn_handler.cc)
cxx_link(rpc base asio_fiber_lib strings absl_hash absl_flat_hash_map
         status TRDP::protobuf)

add_library(rpc_test_lib rpc_test_utils.cc)
cxx_link(rpc_test_lib rpc gtest_main)

cxx_test(rpc_test rpc_test_lib LABELS CI)
