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
        # NOTE: Bazel's cc_test.tags attribute is non-configurable, so select()
        # cannot be used here. Commit 34c515c3 tried to auto-add asan/tsan/sanitizer
        # tags via select() on is_asan/is_tsan, but that breaks the entire load
        # phase ("attribute 'tags' is not configurable") for every ds_cc_test,
        # making all bazel tests unrunnable. Reverted to the pre-34c515c3 form.
        # The sanitizer auto-tagging intent still stands; it must be implemented
        # without select() in tags (e.g. CI-side selection via --config=asan, or
        # per-target sanitizer aliases). is_asan/is_tsan config_settings and the
        # .bazelrc defines are kept for that future implementation.
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
