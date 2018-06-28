add_library(status status.cc)
cxx_link(status base status_proto)
cxx_proto_lib(status PY)

add_library(util zlib_source.cc bzip_source.cc
            sinksource.cc zstd_sinksource.cc)

cxx_link(util strings status TRDP::lz4 TRDP::zstd bz2 z)


add_library(threads executor.cc sp_task_pool.cc)
cxx_link(threads proc_stats event event_pthreads)

cxx_test(executor_test threads LABELS CI)
cxx_test(sp_task_pool_test base threads util LABELS CI)

add_library(fibers sq_threadpool.cc)
cxx_link(fibers base status ${Boost_LIBRARIES})

add_library(proc_stats proc_stats.cc)
cxx_link(proc_stats strings)

cxx_test(sinksource_test strings util LABELS CI)
cxx_test(sq_threadpool_test base fibers LABELS CI)

add_subdirectory(http)
add_subdirectory(math)
add_subdirectory(coding)
add_subdirectory(plang)
add_subdirectory(pprint)
add_subdirectory(stats)
