load("@rules_cc//cc:defs.bzl", "cc_import", "cc_library")

package(default_visibility = ["//visibility:public"])

licenses(["notice"])

cc_import(
    name = "datasystem_shared",
    shared_library = "lib/libdatasystem.so",
)

cc_library(
    name = "headers",
    hdrs = glob(["**/*.h"]),
    includes = ["include"],
)

cc_library(
    name = "datasystem_sdk",
    deps = [
        ":datasystem_shared",
        ":headers",
    ],
)
