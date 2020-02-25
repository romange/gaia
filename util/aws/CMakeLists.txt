find_package(LibXml2)

include_directories(${LIBXML2_INCLUDE_DIR})

add_library(aws_lib aws.cc s3.cc)
cxx_link(aws_lib asio_fiber_lib file status https_client_lib ${LIBXML2_LIBRARIES})

