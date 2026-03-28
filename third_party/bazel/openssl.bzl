# Openssl Bazel build rules

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:utils.bzl", "maybe")

def setup_openssl():
    """Setup Openssl library for Bazel builds."""
    maybe(
        http_archive,
        name = "openssl",
        sha256 = "8dee9b24bdb1dcbf0c3d1e9b02fb8f6bf22165e807f45adeb7c9677536859d3b",
        strip_prefix = "openssl-1.1.1t",
        urls = [
            "https://github.com/openssl/openssl/releases/download/OpenSSL_1_1_1t/openssl-1.1.1t.tar.gz",
        ],
        build_file = "//third_party:openssl.BUILD",
    )
