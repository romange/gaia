add_subdirectory(math)
add_subdirectory(coding)

add_library(status status.cc)
cxx_link(status base status_proto)
cxx_proto_lib(status PY)

add_library(util zlib_source.cc # crc32c.cc
            sinksource.cc zstd_sinksource.cc)

cxx_link(util strings z status TRDP::lz4 TRDP::zstd)


add_library(task_pool sp_task_pool.cc)
cxx_link(task_pool base TRDP::folly)

add_library(fibers sq_threadpool.cc)
cxx_link(fibers base ${Boost_LIBRARIES})
add_include(fibers ${Boost_INCLUDE_DIR})

cxx_test(sp_task_pool_test base task_pool LABELS CI)
cxx_test(sinksource_test strings util LABELS CI)

