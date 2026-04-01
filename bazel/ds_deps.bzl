load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:utils.bzl", "maybe")
load(":ds_python_deps.bzl", "ds_python_deps")
load(":grpc_deps.bzl", "grpc_deps")

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
    setup_nlohmann_json()
    setup_rocksdb()
    setup_curl()

def setup_nlohmann_json():
    maybe(
        http_archive,
        name = "nlohmann_json",
        sha256 = "0d8ef5af7f9794e3263480193c491549b2ba6cc74bb018906202ada498a79406",
        strip_prefix = "json-3.11.3",
        urls = [
            "https://github.com/nlohmann/json/archive/v3.11.3.tar.gz",
        ],
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

def setup_spdlog():
    """Setup spdlog library for Bazel builds."""
    maybe(
        http_archive,
        name = "ds-spdlog",
        sha256 = "4dccf2d10f410c1e2feaff89966bfc49a1abb29ef6f08246335b110e001e09a9",
        strip_prefix = "spdlog-1.12.0",
        urls = [
            "https://github.com/gabime/spdlog/archive/v1.12.0.tar.gz",
        ],
        patches = [
            "@yuanrong-datasystem//third_party/patches/spdlog:change-namespace.patch",
            "@yuanrong-datasystem//third_party/patches/spdlog:change-rotating-file-sink.patch",
        ],
        patch_args = ["-p1"],
        build_file = "@yuanrong-datasystem//third_party:ds-spdlog.BUILD",
    )

def setup_tbb():
    """Setup tbb library for Bazel builds."""
    maybe(
        http_archive,
        name = "tbb",
        sha256 = "ebc4f6aa47972daed1f7bf71d100ae5bf6931c2e3144cf299c8cc7d041dca2f3",
        strip_prefix = "oneTBB-2020.3",
        urls = [
            "https://github.com/uxlfoundation/oneTBB/archive/v2020.3.tar.gz",
        ],
        patches = [
            "@yuanrong-datasystem//third_party/patches/tbb:2020.3/soft-link.patch",
            "@yuanrong-datasystem//third_party/patches/tbb:2020.3/adapt-task.h-to-gcc-14.patch",
        ],
        patch_args = ["-p1"],
        build_file = "@yuanrong-datasystem//third_party:tbb.BUILD",
    )

def setup_zmq():
    """Setup zmq library for Bazel builds."""
    maybe(
        http_archive,
        name = "zmq",
        sha256 = "6c972d1e6a91a0ecd79c3236f04cf0126f2f4dfbbad407d72b4606a7ba93f9c6",
        strip_prefix = "libzmq-4.3.5",
        urls = [
            "https://github.com/zeromq/libzmq/archive/v4.3.5.tar.gz",
        ],
        build_file = "@yuanrong-datasystem//third_party:zmq.BUILD",
    )

def setup_jemalloc():
    """Setup Jemalloc library for Bazel builds."""
    maybe(
        http_archive,
        name = "jemalloc",
        sha256 = "ef6f74fd45e95ee4ef7f9e19ebe5b075ca6b7fbe0140612b2a161abafb7ee179",
        strip_prefix = "jemalloc-5.3.0",
        urls = [
            "https://github.com/jemalloc/jemalloc/archive/refs/tags/5.3.0.tar.gz",
        ],
        build_file = "@yuanrong-datasystem//third_party:jemalloc.BUILD",
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
        strip_prefix = "pybind11_bazel-2.11.1",
        urls = [
            "https://github.com/pybind/pybind11_bazel/releases/download/v2.11.1/pybind11_bazel-2.11.1.zip",
        ],
        sha256 = "2c466c9b3cca7852b47e0785003128984fcf0d5d61a1a2e4c5aceefd935ac220",
    )
    maybe(
        http_archive,
        name = "pybind11",
        strip_prefix = "pybind11-2.11.1",
        urls = [
            "https://github.com/pybind/pybind11/archive/refs/tags/v2.11.1.zip",
        ],
        build_file = "@pybind11_bazel//:pybind11.BUILD",
        sha256 = "b011a730c8845bfc265f0f81ee4e5e9e1d354df390836d2a25880e123d021f89",
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
    maybe(
        http_archive,
        name = "abseil-cpp",
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
    )

def setup_rocksdb():
    maybe(
        # 7.10.2
        http_archive,
        name = "rocksdb",
        sha256 = "4619ae7308cd3d11cdd36f0bfad3fb03a1ad399ca333f192b77b6b95b08e2f78",
        strip_prefix = "rocksdb-7.10.2",
        urls = [
            "https://github.com/facebook/rocksdb/archive/refs/tags/v7.10.2.tar.gz",
        ],
        patches = [
            "@yuanrong-datasystem//third_party/patches/rocksdb:include-algorithm-for-gcc-14.patch",
        ],
        patch_args = ["-p1"],
        build_file = "@yuanrong-datasystem//third_party:rocksdb.BUILD",
    )

def setup_curl():
    maybe(
        # 8.8.0
        http_archive,
        name = "curl",
        sha256 = "77c0e1cd35ab5b45b659645a93b46d660224d0024f1185e8a95cdb27ae3d787d",
        strip_prefix = "curl-8.8.0",
        urls = [
            "https://github.com/curl/curl/releases/download/curl-8_8_0/curl-8.8.0.tar.gz",
        ],
        build_file = "@yuanrong-datasystem//third_party:curl.BUILD",
    )
