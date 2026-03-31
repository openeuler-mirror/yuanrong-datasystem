load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:utils.bzl", "maybe")

def ds_python_deps():
    maybe(
        http_archive,
        name = "rules_python",
        sha256 = "3b8b4cdc991bc9def8833d118e4c850f1b7498b3d65d5698eea92c3528b8cf2c",
        strip_prefix = "rules_python-0.30.0",
        url = "https://github.com/bazelbuild/rules_python/releases/download/0.30.0/rules_python-0.30.0.tar.gz",
    )
