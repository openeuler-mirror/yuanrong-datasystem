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
