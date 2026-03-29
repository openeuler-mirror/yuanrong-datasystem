load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:utils.bzl", "maybe")

def grpc_deps():
    maybe(
        http_archive,
        name = "bazel_features",
        sha256 = "5ac743bf5f05d88e84962e978811f2524df09602b789c92cf7ae2111ecdeda94",
        strip_prefix = "bazel_features-1.14.0",
        urls = [
            "https://github.com/bazel-contrib/bazel_features/releases/download/v1.14.0/bazel_features-v1.14.0.tar.gz",
        ],
    )
    maybe(
        http_archive,
        name = "com_google_googleapis",
        sha256 = "0513f0f40af63bd05dc789cacc334ab6cec27cc89db596557cb2dfe8919463e4",
        strip_prefix = "googleapis-fe8ba054ad4f7eca946c2d14a63c3f07c0b586a0",
        build_file = Label("//third_party:googleapis.BUILD"),
        urls = [
            "https://github.com/googleapis/googleapis/archive/fe8ba054ad4f7eca946c2d14a63c3f07c0b586a0.tar.gz",
        ],
    )
    maybe(
        http_archive,
        name = "com_github_cncf_xds",
        sha256 = "dc305e20c9fa80822322271b50aa2ffa917bf4fd3973bcec52bfc28dc32c5927",
        strip_prefix = "xds-3a472e524827f72d1ad621c4983dd5af54c46776",
        urls = [
            "https://github.com/cncf/xds/archive/3a472e524827f72d1ad621c4983dd5af54c46776.tar.gz",
        ],
        patches = [
            "@yuanrong-datasystem//third_party/patches/grpc:1.68.2/xds_remove_deps_46776.patch",
        ],
        patch_args = ["-p1"],
    )
    maybe(
        http_archive,
        name = "com_envoyproxy_protoc_gen_validate",
        sha256 = "c695fc5a2e5a1b52904cd8a58ce7a1c3a80f7f50719496fd606e551685c01101",
        strip_prefix = "protoc-gen-validate-0.6.1",
        urls = ["https://github.com/envoyproxy/protoc-gen-validate/archive/refs/tags/v0.6.1.tar.gz"],
    )
    maybe(
        http_archive,
        name = "io_bazel_rules_go",
        urls = [
            "https://github.com/bazelbuild/rules_go/releases/download/v0.39.1/rules_go-v0.39.1.zip",
        ],
        sha256 = "6dc2da7ab4cf5d7bfc7c949776b1b7c733f05e56edc4bcd9022bb249d2e2a996",
    )
