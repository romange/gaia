cc_library(
    name = "zstd",
    srcs = glob([
        "common/*.c",
        "common/*.h",
        "compress/*.c",
        "compress/*.h",
        "decompress/*.c",
        "decompress/*.h",
    ]),
    hdrs = ["zstd.h"],
    includes = [
        ".",
        "common",
    ],
    visibility = ["//visibility:public"],
)
