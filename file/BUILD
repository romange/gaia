cc_library(
    name = "file",
    srcs = [
        "compressors.cc",
        "file.cc",
        "file_util.cc",
        "filesource.cc",
        "gzip_file.cc",
        "list_file.cc",
        "list_file_reader.cc",
        "lst2_impl.cc",
        "meta_map_block.cc",
    ],
    hdrs = glob(["*.h"]),
    visibility = ["//visibility:public"],
    deps = [
        "//base",
        "//strings",
        "//util",
        "@crc32c",
        "@lz4",
    ],
)
