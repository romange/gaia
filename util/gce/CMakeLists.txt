add_library(gce_lib gce.cc gcs.cc gcs_read_file.cc)
cxx_link(gce_lib asio_fiber_lib file status https_client_lib TRDP::rapidjson)

