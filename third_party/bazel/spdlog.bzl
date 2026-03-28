load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:utils.bzl", "maybe")

def setup_spdlog():
    """Setup spdlog library for Bazel builds."""
    maybe(
        http_archive,
        name = "ds-spdlog",
        sha256 = "4dccf2d10f410c1e2feaff89966bfc49a1abb29ef6f08246335b110e001e09a9",
        strip_prefix = "spdlog-1.12.0",
        urls = [
            "https://github.com/gabime/spdlog/archive/v1.12.0.tar.gz",
        ],
        patches = [
            "//third_party/patches/spdlog:change-namespace.patch",
            "//third_party/patches/spdlog:change-rotating-file-sink.patch"
        ],
        patch_args = ["-p1"],
        build_file_content = """
# Generated BUILD file for spdlog with security options
package(default_visibility = ["//visibility:public"])

# Load cc rules
load("@rules_cc//cc:defs.bzl", "cc_library")

cc_library(
    name = "ds-spdlog",
    srcs = glob([
        "src/*.cpp",
    ]),
    hdrs = glob(["include/**/*.h", "include/**/*.hpp"]),
    strip_include_prefix = "include",
    copts = [
        "-DSPDLOG_COMPILED_LIB",  # Required macro for compiling spdlog as a library
        "-std=c++11",  # Standard C++11 for compatibility
    ],
    visibility = ["//visibility:public"],
)
""",
    )
