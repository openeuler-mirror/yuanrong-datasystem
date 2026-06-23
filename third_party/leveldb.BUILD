package(default_visibility = ["//visibility:public"])
load("@rules_cc//cc:defs.bzl", "cc_library")

cc_library(
    name = "leveldb",
    srcs = glob(
        [
            "db/*.cc",
            "table/*.cc",
            "util/*.cc",
            "port/*.cc",
            "helpers/memenv/*.cc",
        ],
        exclude = [
            "db/*_test.cc",
            "db/*_benchmark.cc",
            "db/leveldbutil.cc",
            "table/*_test.cc",
            "util/*_test.cc",
            "util/testutil.cc",
            "util/env_windows.cc",
            "helpers/memenv/*_test.cc",
        ],
    ),
    hdrs = glob([
        "include/leveldb/*.h",
        "db/*.h",
        "table/*.h",
        "util/*.h",
        "port/*.h",
        "helpers/memenv/*.h",
    ]),
    includes = ["include"],
    copts = [
        "-fPIC",
        "-std=c++17",
        "-DLEVELDB_PLATFORM_POSIX",
    ],
    visibility = ["//visibility:public"],
)
