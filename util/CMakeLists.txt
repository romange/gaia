cxx_proto_lib(status PY)

add_library(status status.cc)
cxx_link(status base status_proto)

add_library(util zlib_source.cc bzip_source.cc
            sinksource.cc zstd_sinksource.cc)

add_library(pb2json pb2json.cc)
cxx_link(pb2json strings status TRDP::protobuf)
add_dependencies(pb2json rapidjson_project)

cxx_link(util strings status TRDP::lz4 TRDP::zstd bz2 z)


add_library(threads sp_task_pool.cc)
cxx_link(threads base)

cxx_test(sp_task_pool_test base threads util LABELS CI)

add_library(fibers sq_threadpool.cc)
cxx_link(fibers base status strings Boost::fiber Boost::thread)

add_library(proc_stats proc_stats.cc spawn.cc)
cxx_link(proc_stats strings)

cxx_test(sinksource_test strings util LABELS CI)
cxx_test(sq_threadpool_test fibers LABELS CI)
cxx_test(pb2json_test pb2json addressbook_proto LABELS CI)

add_subdirectory(asio)
add_subdirectory(http)
add_subdirectory(math)
add_subdirectory(coding)
add_subdirectory(plang)
add_subdirectory(pprint)
add_subdirectory(rpc)
add_subdirectory(sentry)
add_subdirectory(stats)
