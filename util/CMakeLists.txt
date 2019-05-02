cxx_proto_lib(status PY)

add_library(status status.cc)
cxx_link(status base status_proto)

add_library(util zlib_source.cc bzip_source.cc
            sinksource.cc zstd_sinksource.cc)
cxx_link(util strings status TRDP::lz4 TRDP::zstd bz2 TRDP::intel_z)

add_library(pb2json pb2json.cc)
cxx_link(pb2json strings status TRDP::protobuf TRDP::rapidjson absl_variant)
add_dependencies(pb2json rapidjson_project)

add_library(sp_task_pool sp_task_pool.cc)
cxx_link(sp_task_pool base)

cxx_test(sp_task_pool_test base sp_task_pool util LABELS CI)

add_library(proc_stats proc_stats.cc spawn.cc)
cxx_link(proc_stats strings)

cxx_test(sinksource_test strings util LABELS CI)
cxx_test(pb2json_test pb2json addressbook_proto LABELS CI)

add_subdirectory(asio)
add_subdirectory(fibers)
add_subdirectory(http)
add_subdirectory(html)
add_subdirectory(math)
add_subdirectory(coding)
add_subdirectory(plang)
add_subdirectory(pprint)
add_subdirectory(rpc)
add_subdirectory(sentry)
add_subdirectory(stats)
add_subdirectory(gce)
