# nlohmann/json Bazel build rules

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:utils.bzl", "maybe")

def setup_nlohmann_json():
    """Setup nlohmann/json library for Bazel builds."""
    maybe(
        http_archive,
        name = "nlohmann_json",
        sha256 = "0d8ef5af7f9794e3263480193c491549b2ba6cc74bb018906202ada498a79406",
        strip_prefix = "json-3.11.3",
        urls = [
            "https://github.com/nlohmann/json/archive/v3.11.3.tar.gz",
        ],
    )
