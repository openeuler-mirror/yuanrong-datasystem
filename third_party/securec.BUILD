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