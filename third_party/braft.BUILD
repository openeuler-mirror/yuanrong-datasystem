load("@com_google_protobuf//bazel:cc_proto_library.bzl", "cc_proto_library")
load("@rules_cc//cc:defs.bzl", "cc_library")
load("@rules_proto//proto:defs.bzl", "proto_library")

package(default_visibility = ["//visibility:public"])

licenses(["notice"])

config_setting(
    name = "linux_x86_64",
    constraint_values = [
        "@platforms//cpu:x86_64",
        "@platforms//os:linux",
    ],
)

proto_library(
    name = "braft_proto",
    srcs = glob(["src/braft/*.proto"]),
    strip_import_prefix = "/src",
)

cc_proto_library(
    name = "braft_cc_proto",
    deps = [":braft_proto"],
)

cc_library(
    name = "braft",
    srcs = glob(["src/braft/*.cpp"]),
    hdrs = glob([
        "src/braft/*.h",
        "src/braft/*.hpp",
    ]),
    copts = [
        "-fPIC",
        "-fno-omit-frame-pointer",
        "-Wno-invalid-offsetof",
        "-Wno-missing-field-initializers",
        "-Wno-reserved-user-defined-literal",
        "-Wno-sign-compare",
        "-Wno-unused-parameter",
        "-Wno-unused-variable",
    ] + select({
        ":linux_x86_64": [
            "-msse4",
            "-msse4.2",
        ],
        "//conditions:default": [],
    }),
    local_defines = [
        "BRAFT_REVISION=\\\"v1.1.2\\\"",
        "BRPC_WITH_GLOG=0",
        "BTHREAD_USE_FAST_PTHREAD_MUTEX",
        "GFLAGS_NS=google",
        "NO_TCMALLOC",
        "USE_SYMBOLIZE",
        "_GNU_SOURCE",
        "__STDC_CONSTANT_MACROS",
        "__STDC_FORMAT_MACROS",
        "__STDC_LIMIT_MACROS",
        "__STRICT_ANSI__",
        "__const__=__unused__",
    ],
    includes = ["src"],
    linkopts = [
        "-ldl",
        "-lm",
        "-lpthread",
        "-lrt",
    ],
    deps = [
        ":braft_cc_proto",
        "@com_github_apache_brpc//:brpc",
        "@com_github_gflags_gflags//:gflags",
        "@com_github_google_leveldb//:leveldb",
        "@com_google_protobuf//:protobuf",
        "@zlib//:zlib",
    ],
)
