add_library(aws_lib aws.cc)
cxx_link(aws_lib asio_fiber_lib file status https_client_lib)

