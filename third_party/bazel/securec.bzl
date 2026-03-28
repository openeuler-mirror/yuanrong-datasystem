load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:utils.bzl", "maybe")

def setup_securec():
    """Setup securec library for Bazel builds."""
    maybe(
        http_archive,
        name = "securec",
        sha256 = "f5fb2679a46b71e39bab106814952a63310803e4424aa9a5a9b72a1f12d047d6",
        strip_prefix = "libboundscheck-1.1.16",
        urls = [
            "https://github.com/openeuler-mirror/libboundscheck/archive/v1.1.16.tar.gz",
        ],
        patches = ["//third_party/patches/securec:libboundscheck-cmake-support.patch"],
        patch_args = ["-p1"],
        build_file_content = """
# Generated BUILD file for Secure C Library with security options
package(default_visibility = ["//visibility:public"])

# Load cc rules
load("@rules_cc//cc:defs.bzl", "cc_library")

cc_library(
    name = "securec",
    srcs = glob([
        "src/*.c",
        "src/*.h",
        "src/*.inl",
    ]),
    hdrs = glob(["include/*.h"]),
    strip_include_prefix = "include",
    visibility = ["//visibility:public"],
    copts = [
        "-std=c99",  # Use C99 standard for C files, not C++ standard
        "-Wno-error",  # Prevent warnings from being treated as errors if needed
    ],
)
""",
    )
