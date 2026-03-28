# RE2 Bazel build rules

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:utils.bzl", "maybe")

def setup_re2():
    """Setup RE2 library for Bazel builds."""
    maybe(
        http_archive,
        name = "com_googlesource_code_re2",
        sha256 = "7a9a4824958586980926a300b4717202485c4b4115ac031822e29aa4ef207e48",
        strip_prefix = "re2-2023-03-01",
        urls = [
            "https://github.com/google/re2/archive/refs/tags/2023-03-01.tar.gz",
        ],
        repo_mapping = {"@abseil-cpp": "@com_google_absl"},
    )
