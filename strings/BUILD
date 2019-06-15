cc_library(
    name = "strings",
    srcs = glob(
        ["*.cc"],
        exclude = ["strpmr.cc", "*_test.cc"]
    ),
    hdrs = glob(["*.h"]),
    visibility = ["//visibility:public"],
    deps = [
        "//base",
        "@com_google_absl//absl/strings",
        "@sparsehash",
    ],
)

cc_library(
    name = "strpmr",
    srcs = [
        "strpmr.cc",
    ],
    hdrs = glob(["*.h"]),
    visibility = ["//visibility:public"],
    deps = [
        ":strings",
        "//:pmr",
    ],
)

test_deps = [
    ":strings",

    "@com_google_gtest//:gtest",
    "@com_google_gtest//:gtest_main",
    "@com_google_benchmark//:benchmark",
]

cc_test(
    name = "range_test",
    srcs = [
        "range_test.cc",
    ],
    deps = test_deps,
)

cc_test(
    name = "unique_strings_test",
    srcs = [
        "unique_strings_test.cc",
    ],
    deps = test_deps,
)

cc_test(
    name = "strcat_test",
    srcs = [
        "strcat_test.cc",
    ],
    deps = test_deps,
)

cc_test(
    name = "strpmr_test",
    srcs = [
        "strpmr_test.cc",
    ],
    deps = test_deps + [":strpmr"],
)
