load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")


def base_rules():
    http_archive(
        name="build_bazel_rules_swift",
        url="https://github.com/bazelbuild/rules_swift/releases/download/1.18.0/rules_swift.1.18.0.tar.gz",
        sha256="bb01097c7c7a1407f8ad49a1a0b1960655cf823c26ad2782d0b7d15b323838e2",
    )

    http_archive(
        name="rules_ruby",
        sha256="347927fd8de6132099fcdc58e8f7eab7bde4eb2fd424546b9cd4f1c6f8f8bad8",
        strip_prefix="rules_ruby-b7f3e9756f3c45527be27bc38840d5a1ba690436",
        urls=[
            "https://github.com/protocolbuffers/rules_ruby/archive/b7f3e9756f3c45527be27bc38840d5a1ba690436.zip"
        ],
    )

    http_archive(
        name="rules_pkg",
        sha256="038f1caa773a7e35b3663865ffb003169c6a71dc995e39bf4815792f385d837d",
        urls=[
            "https://mirror.bazel.build/github.com/bazelbuild/rules_pkg/releases/download/0.4.0/rules_pkg-0.4.0.tar.gz",
        ],
    )

    http_archive(
        name="rules_python",
        sha256="ca77768989a7f311186a29747e3e95c936a41dffac779aff6b443db22290d913",
        strip_prefix="rules_python-0.36.0",
        url="https://github.com/bazel-contrib/rules_python/releases/download/0.36.0/rules_python-0.36.0.tar.gz",
    )

    http_archive(
        name="platforms",
        urls=[
            "https://github.com/bazelbuild/platforms/releases/download/1.0.0/platforms-1.0.0.tar.gz",
        ],
        sha256="3384eb1c30762704fbe38e440204e114154086c8fc8a8c2e3e28441028c019a8",
    )

    http_archive(
        name="io_bazel_rules_go",
        sha256="6dc2da7ab4cf5d7bfc7c949776b1b7c733f05e56edc4bcd9022bb249d2e2a996",
        urls=[
            "https://mirror.bazel.build/github.com/bazelbuild/rules_go/releases/download/v0.39.1/rules_go-v0.39.1.zip",
        ],
    )