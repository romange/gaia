load("@rules_foreign_cc//tools/build_defs:cmake.bzl", "cmake_external")
load("@rules_foreign_cc//tools/build_defs:configure.bzl", "configure_make")

cmake_external(
    name = "pmr",
    lib_source = "@pmr//:all",
    static_libraries = ["libpmr.a"],
    visibility = ["//visibility:public"],
)

cc_library(
    name = "sparsehash_config",
    hdrs = ["sparseconfig.h"],
    include_prefix = "sparsehash/internal",
    visibility = ["//visibility:public"],
)

exports_files(["crc32c_config.h"])
