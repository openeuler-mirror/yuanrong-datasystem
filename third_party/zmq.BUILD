package(default_visibility = ["//visibility:public"])
load("@rules_cc//cc:defs.bzl", "cc_library")

genrule(
    name = "platform_hpp",
    srcs = ["@yuanrong-datasystem//third_party/bazel:platform.hpp"],
    outs = ["platform.hpp"],
    cmd = "cp $(location @yuanrong-datasystem//third_party/bazel:platform.hpp) $@",
)

cc_library(
    name = "zmq",
    srcs = glob([
        "src/*.cpp",
    ], exclude=[
        "src/ws_*.cpp",
         "src/ws_*.hpp",
         "src/wss_*.cpp",
         "src/wss_*.hpp",
         "src/vmci_*.cpp",
         "src/vmci_*.hpp",
         #"src/tipc_*.cpp",
         #"src/tipc_*.hpp"
    ]) + glob(["external/sha1/*.c"]),
    hdrs = glob(["include/*.h", "src/*.hpp", "src/*.h", "external/sha1/*.h"],
    exclude=[
         "src/wss_*.hpp",
    ]) + [":platform_hpp"],
    includes = [
        "include",
        "src",
        ".",
        "external"
    ],
    defines = [
            "ZMQ_HAVE_STRLCPY=1",
    ],
    copts = [
        "-fPIC",
        "-std=c++17",
        "-Wno-error",
    ],
    linkopts = [
        "-lpthread"
    ],
    visibility = ["//visibility:public"],
)
