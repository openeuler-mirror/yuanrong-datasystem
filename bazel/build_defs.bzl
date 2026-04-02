def ds_cc_library(name, srcs = None, hdrs = None, copts = [], deps = None, **kwargs):
    native.cc_library(
        name = name,
        srcs = srcs or [],
        hdrs = hdrs or [],
        strip_include_prefix = "/src",
        deps = deps or [],
        copts = copts,
        **kwargs
    )

def ds_cc_test(
        name,
        srcs,
        deps = [],
        data = [],
        copts = [],
        defines = [],
        linkopts = [],
        tags = [],
        timeout = "moderate",
        **kwargs):
    native.cc_test(
        name = name,
        srcs = srcs,
        deps = deps + [
            "@com_google_googletest//:gtest",
            "@com_google_googletest//:gtest_main",
        ],
        data = data,
        copts = copts + [
            "-std=c++17",
            "-g",
            "-O0",
            "-Itests",
        ],
        defines = defines + ["WITH_TESTS=1"],
        linkopts = linkopts + [
            "-pthread",
        ],
        tags = tags + ["ds_test"],
        timeout = timeout,
        **kwargs
    )

def ds_cc_test_library(
        name,
        srcs = [],
        hdrs = [],
        deps = [],
        defines = [],
        visibility = None,
        **kwargs):
    native.cc_library(
        name = name,
        srcs = srcs,
        hdrs = hdrs,
        deps = deps + [
            "@com_google_googletest//:gtest",
        ],
        defines = defines + ["WITH_TESTS=1"],
        includes = ["."],
        visibility = visibility or ["//tests:__subpackages__"],
        **kwargs
    )
