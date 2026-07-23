load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:local.bzl", "new_local_repository")
load("@bazel_tools//tools/build_defs/repo:utils.bzl", "maybe")
load(":ds_python_deps.bzl", "ds_python_deps")
load(":grpc_deps.bzl", "grpc_deps")
load(":cuda_local_repo.bzl", "cuda_local_repository")
load(
    "//bazel/sdk:repositories.bzl",
    "setup_braft",
    "setup_brpc",
    "setup_curl",
    "setup_gflags",
    "setup_jemalloc",
    "setup_leveldb",
    "setup_nlohmann_json",
    "setup_rocksdb",
    "setup_spdlog",
    "setup_tbb",
    "setup_zmq",
)

def ds_deps():
    """Loads dependencies need to compile and test the Yuanrong Datasystem ."""

    setup_rules()
    setup_protobuf()
    grpc_deps()
    ds_python_deps()
    setup_pybind11()
    setup_zlib()
    setup_re2()
    setup_gtest()
    setup_absl()

    #setup_openssl()
    setup_boringssl()
    setup_jemalloc()
    setup_spdlog()
    setup_securec()
    setup_tbb()
    setup_zmq()
    setup_gflags()
    setup_leveldb()
    setup_brpc()
    setup_braft()
    setup_nlohmann_json()
    setup_rocksdb()
    setup_curl()
    setup_cuda()
    setup_mlcachedirect()
    setup_local_urma()

def setup_cuda():
    maybe(
        cuda_local_repository,
        name = "local_cuda",
    )

def setup_openssl():
    maybe(
        http_archive,
        name = "openssl",
        sha256 = "f89199be8b23ca45fc7cb9f1d8d3ee67312318286ad030f5316aca6462db6c96",
        strip_prefix = "openssl-1.1.1m",
        urls = [
            "https://github.com/openssl/openssl/releases/download/OpenSSL_1_1_1m/openssl-1.1.1m.tar.gz",
        ],
        build_file = "@yuanrong-datasystem//third_party:openssl.BUILD",
    )

def setup_boringssl():
    maybe(
        http_archive,
        name = "boringssl",
        sha256 = "c70d519e4ee709b7a74410a5e3a937428b8198d793a3d771be3dd2086ae167c8",
        strip_prefix = "boringssl-b8b3e6e11166719a8ebfa43c0cde9ad7d57a84f6",
        urls = [
            "https://github.com/google/boringssl/archive/b8b3e6e11166719a8ebfa43c0cde9ad7d57a84f6.tar.gz",
        ],
    )

def setup_grpc():
    maybe(
        http_archive,
        name = "com_github_grpc_grpc",
        sha256 = "afbc5d78d6ba6d509cc6e264de0d49dcd7304db435cbf2d630385bacf49e066c",
        strip_prefix = "grpc-1.68.2",
        urls = [
            "https://github.com/grpc/grpc/archive/refs/tags/v1.68.2.tar.gz",
        ],
        patches = [
            "@yuanrong-datasystem//third_party/patches/grpc:1.68.2/grpc_1_68_2_bzl.patch",
            "@yuanrong-datasystem//third_party/patches/grpc:1.68.2/grpc_remove_deps_1_68_2.patch",
        ],
        patch_args = ["-p1"],
    )

def setup_securec():
    """Setup securec library for Bazel builds."""
    maybe(
        http_archive,
        name = "securec",
        sha256 = "f5fb2679a46b71e39bab106814952a63310803e4424aa9a5a9b72a1f12d047d6",
        strip_prefix = "libboundscheck-1.1.16",
        urls = [
            "https://github.com/openeuler-mirror/libboundscheck/archive/v1.1.16.tar.gz",
        ],
        patches = ["@yuanrong-datasystem//third_party/patches/securec:libboundscheck-cmake-support.patch"],
        patch_args = ["-p1"],
        build_file = "@yuanrong-datasystem//third_party:securec.BUILD",
    )

def setup_rules():
    maybe(
        http_archive,
        name = "rules_cc",
        sha256 = "b8b918a85f9144c01f6cfe0f45e4f2838c7413961a8ff23bc0c6cdf8bb07a3b6",
        strip_prefix = "rules_cc-0.1.5",
        urls = [
            "https://github.com/bazelbuild/rules_cc/releases/download/0.1.5/rules_cc-0.1.5.tar.gz",
        ],
        patches = [
            "@yuanrong-datasystem//third_party/patches/bazel_rules:rules_cc_0_1_5_set_std_c++17.diff",
        ],
        patch_args = ["-p1"],
    )
    maybe(
        http_archive,
        name = "rules_proto",
        sha256 = "6fb6767d1bef535310547e03247f7518b03487740c11b6c6adb7952033fe1295",
        strip_prefix = "rules_proto-6.0.2",
        urls = [
            "https://github.com/bazelbuild/rules_proto/archive/refs/tags/6.0.2.tar.gz",
        ],
        patches = [
            "@yuanrong-datasystem//third_party/patches/bazel_rules:rules_proto_6_0_2_remove_deps.patch",
        ],
        patch_args = ["-p1"],
    )

    maybe(
        http_archive,
        name = "rules_pkg",
        urls = [
            "https://github.com/bazelbuild/rules_pkg/releases/download/0.7.0/rules_pkg-0.7.0.tar.gz",
        ],
        sha256 = "8a298e832762eda1830597d64fe7db58178aa84cd5926d76d5b744d6558941c2",
    )
    maybe(
        http_archive,
        name = "bazel_skylib",
        urls = [
            "https://github.com/bazelbuild/bazel-skylib/releases/download/1.7.1/bazel-skylib-1.7.1.tar.gz",
        ],
        sha256 = "bc283cdfcd526a52c3201279cda4bc298652efa898b10b4db0837dc51652756f",
    )

def setup_pybind11():
    maybe(
        http_archive,
        name = "pybind11_bazel",
        strip_prefix = "pybind11_bazel-2.13.6",
        urls = [
            "https://github.com/pybind/pybind11_bazel/archive/refs/tags/v2.13.6.tar.gz",
        ],
        sha256 = "cae680670bfa6e82703c03f2a3c995408cdcbf43616d7bdd198ef45d3c327731",
    )
    maybe(
        http_archive,
        name = "pybind11",
        strip_prefix = "pybind11-2.13.6",
        urls = [
            "https://github.com/pybind/pybind11/archive/refs/tags/v2.13.6.tar.gz",
        ],
        build_file = "@pybind11_bazel//:pybind11-BUILD.bazel",
        sha256 = "e08cb87f4773da97fa7b5f035de8763abc656d87d5773e62f6da0587d1f0ec20",
    )

def setup_gtest():
    maybe(
        http_archive,
        name = "com_google_googletest",
        sha256 = "81964fe578e9bd7c94dfdb09c8e4d6e6759e19967e397dbea48d1c10e45d0df2",
        strip_prefix = "googletest-release-1.12.1",
        urls = [
            "https://github.com/google/googletest/archive/refs/tags/release-1.12.1.tar.gz",
        ],
    )

def setup_protobuf():
    maybe(
        http_archive,
        name = "com_google_protobuf",
        sha256 = "b3b4c3b6cfe74b77aeae9909fc3c1303092717f71bc2154d7c1961acdaf5fe4c",
        strip_prefix = "protobuf-28.3",
        urls = [
            "https://github.com/protocolbuffers/protobuf/releases/download/v28.3/protobuf-28.3.zip",
        ],
        patches = ["@yuanrong-datasystem//third_party/patches/protobuf:28.3/protobuf_remove_deps_28_3.patch"],
        patch_args = ["-p1"],
    )

def setup_zlib():
    maybe(
        http_archive,
        name = "zlib",
        build_file = "@yuanrong-datasystem//third_party:zlib.BUILD",
        sha256 = "9a93b2b7dfdac77ceba5a558a580e74667dd6fede4585b91eefb60f03b72df23",
        strip_prefix = "zlib-1.3.1",
        urls = [
            "https://github.com/madler/zlib/releases/download/v1.3.1/zlib-1.3.1.tar.gz",
        ],
    )

def setup_absl():
    maybe(
        http_archive,
        name = "com_google_absl",
        sha256 = "b396401fd29e2e679cace77867481d388c807671dc2acc602a0259eeb79b7811",
        strip_prefix = "abseil-cpp-20250127.1",
        urls = [
            "https://github.com/abseil/abseil-cpp/releases/download/20250127.1/abseil-cpp-20250127.1.tar.gz",
        ],
    )

def setup_re2():
    maybe(
        http_archive,
        name = "re2",
        sha256 = "a835fe55fbdcd8e80f38584ab22d0840662c67f2feb36bd679402da9641dc71e",
        strip_prefix = "re2-2024-07-02",
        urls = [
            "https://github.com/google/re2/releases/download/2024-07-02/re2-2024-07-02.zip",
        ],
        patches = [
            "@yuanrong-datasystem//third_party/patches/re2:modify_deps_absl_namespace.patch",
        ],
        patch_args = ["-p1"],
    )

def setup_mlcachedirect():
    maybe(
        http_archive,
        name = "mlcachedirect",
        sha256 = "3a0501077bdc81c6ffd84420abf41df8b45445b5b455ebc159ac28c61801c690",
        strip_prefix = "MLCacheDirect-0.0.9",
        urls = [
            "https://github.com/openeuler-mirror/MLCacheDirect/archive/refs/tags/v0.0.9.tar.gz",
        ],
    )

def setup_local_urma():
    maybe(
        new_local_repository,
        name = "local_urma",
        path = "/usr",
        build_file = "@mlcachedirect//third_party:BUILD.urma",
    )
