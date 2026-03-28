load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:utils.bzl", "maybe")

def setup_zmq():
    """Setup zmq library for Bazel builds."""
    maybe(
        http_archive,
        name = "zmq",
        sha256 = "6c972d1e6a91a0ecd79c3236f04cf0126f2f4dfbbad407d72b4606a7ba93f9c6",
        strip_prefix = "libzmq-4.3.5",
        urls = [
            "https://github.com/zeromq/libzmq/archive/v4.3.5.tar.gz",
        ],
        build_file_content = """
package(default_visibility = ["//visibility:public"])
load("@rules_cc//cc:defs.bzl", "cc_library")

genrule(
    name = "platform_hpp",
    srcs = ["@//third_party/bazel:platform.hpp"],
    outs = ["platform.hpp"],
    cmd = "cp $(location @//third_party/bazel:platform.hpp) $@",
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
)""",
    )
