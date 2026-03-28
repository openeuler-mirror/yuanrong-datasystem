# Protocol Buffers Bazel build rules

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:utils.bzl", "maybe")

def setup_protobuf():
    """Setup Protocol Buffers library for Bazel builds."""
    maybe(
        http_archive,
        name = "com_google_protobuf",
        sha256 = "4356e78744dfb2df3890282386c8568c85868116317d9b3ad80eb11c2aecf2ff",
        strip_prefix = "protobuf-3.25.5",
        urls = [
            "https://github.com/protocolbuffers/protobuf/archive/v3.25.5.tar.gz",
        ],
        patches = ["//third_party/patches/protobuf/3.25.5:protobuf_support_gcc_7_3.patch"],
        patch_args = ["-p1"],
    )
