# Abseil Bazel build rules

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:utils.bzl", "maybe")

def setup_absl():
    """Setup Abseil library for Bazel builds."""
    maybe(
        http_archive,
        name = "com_google_absl",
        sha256 = "712ebe153a0d0f9f198b93a6168f901d891cec85e7e6285f013e5f3f376ed6e9",
        strip_prefix = "abseil-cpp-lts_2024_07_22",
        urls = [
            "https://github.com/abseil/abseil-cpp/archive/lts_2024_07_22.tar.gz",
        ],
        patch_args = ["-p1"],
        patches = ["//third_party/patches/absl:absl_failure_signal_handler.patch"],
    )
