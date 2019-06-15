proto_library(
    name = "status_proto",
    srcs = [
        "status.proto",
    ],
)

cc_proto_library(
    name = "status_cc_proto",
    deps = [
        ":status_proto",
    ],
)

cc_library(
    name = "status",
    srcs = [
        "status.cc",
    ],
    hdrs = [
        "status.h",
    ],
    deps = [
        ":status_cc_proto",
        "//base",
    ],
)

cc_library(
    name = "util",
    srcs = [
        "bzip_source.cc",
        "sinksource.cc",
        "zlib_source.cc",
        "zstd_sinksource.cc",
    ],
    hdrs = [
        "bzip_source.h",
        "sinksource.h",
        "zlib_source.h",
        "zstd_sinksource.h",
    ],
    linkopts = ["-lbz2"],
    visibility = ["//visibility:public"],
    deps = [
        ":status",
        "//base",
        "//strings",
        "@zstd",
        "@lz4",
    ],
)
