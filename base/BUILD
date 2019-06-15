cc_library(
    name = "base",
    srcs = glob(
        ["*.cc"],
        exclude = [
            "*_test.cc",
            "gtest*",
        ],
    ),
    hdrs = glob(["*.h"]),
    linkopts = ["-lrt"],
    visibility = ["//visibility:public"],
    deps = [
        "//:pmr",
        "@com_github_glog//:glog",
        "@com_google_absl//absl/debugging:failure_signal_handler",
        "@sparsehash",
        "@xxhash",
    ],
)

cc_library(
    name = "gtest",
    srcs = glob(["gtest*"]),
    deps = [
        ":base",
        "@com_google_benchmark//:benchmark",
        "@com_google_gtest//:gtest",
    ],
)

test_deps = [
    ":gtest",
]

cc_test(
    name = "array_test",
    srcs = [
        "array_test.cc",
    ],
    deps = test_deps,
)

cc_test(
    name = "bits_test",
    srcs = [
        "bits_test.cc",
    ],
    deps = test_deps,
)

cc_test(
    name = "pod_array_test",
    srcs = [
        "pod_array_test.cc",
    ],
    deps = test_deps + ["//:pmr"],
)

cc_test(
    name = "arena_test",
    srcs = [
        "arena_test.cc",
    ],
    deps = test_deps + [
        "//:pmr",
        "//strings",
    ],
)

cc_test(
    name = "pmr_test",
    srcs = [
        "pmr_test.cc",
    ],
    deps = test_deps + ["//:pmr"],
)

cc_test(
    name = "simd_test",
    srcs = [
        "simd_test.cc",
    ],
    deps = test_deps,
)

cc_test(
    name = "crc32c_test",
    srcs = [
        "crc32c_test.cc",
    ],
    deps = test_deps + ["//strings"],
)

cc_test(
    name = "walltime_test",
    srcs = [
        "walltime_test.cc",
    ],
    deps = test_deps,
)

cc_test(
    name = "flit_test",
    srcs = [
        "flit_test.cc",
    ],
    deps = test_deps + ["//strings"],
)

cc_test(
    name = "cxx_test",
    srcs = [
        "cxx_test.cc",
    ],
    deps = test_deps,
)

cc_test(
    name = "hash_test",
    srcs = [
        "hash_test.cc",
    ],
    data = ["testdata/ids.txt.gz"],
    deps = test_deps + ["//file"],
)

cc_test(
    name = "RWSpinLock_test",
    srcs = [
        "RWSpinLock_test.cc",
    ],
    deps = test_deps,
)

cc_test(
    name = "event_count_test",
    srcs = [
        "event_count_test.cc",
    ],
    deps = test_deps,
)

cc_test(
    name = "coder_test",
    srcs = [
        "coder_test.cc",
    ],
    deps = test_deps,
)

cc_test(
    name = "wheel_timer_test",
    srcs = [
        "wheel_timer_test.cc",
    ],
    deps = test_deps,
)

cc_test(
    name = "lambda_test",
    srcs = [
        "lambda_test.cc",
    ],
    deps = test_deps,
)

cc_test(
    name = "mpmc_bounded_queue_test",
    srcs = [
        "mpmc_bounded_queue_test.cc",
    ],
    deps = test_deps,
)
