# Grpc Bazel build rules

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:utils.bzl", "maybe")

def setup_grpc():
    """Setup Grpc library for Bazel builds."""
    maybe(
        http_archive,
        name = "com_github_grpc_grpc",
        sha256 = "853b4ff0e1c3e1c4e19f8cc77bbab402981920997716003cea6db9970657f8c9",
        strip_prefix = "grpc-1.65.4",
        urls = [
            "https://github.com/grpc/grpc/archive/v1.65.4.tar.gz",
        ],
    )
