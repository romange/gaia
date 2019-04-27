add_library(gce_lib gce.cc gcs.cc)
cxx_link(gce_lib asio_fiber_lib file status ssl crypto)

