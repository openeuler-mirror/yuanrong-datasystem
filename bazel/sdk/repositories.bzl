"""Repository rules shared by standalone DataSystem and embedded SDK builds."""

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:utils.bzl", "maybe")

def setup_spdlog(name = "ds-spdlog"):
    """Setup spdlog library for Bazel builds."""
    maybe(
        http_archive,
        name = name,
        sha256 = "4dccf2d10f410c1e2feaff89966bfc49a1abb29ef6f08246335b110e001e09a9",
        strip_prefix = "spdlog-1.12.0",
        urls = [
            "https://openyuanrong.obs.cn-southwest-2.myhuaweicloud.com/thirdparty/github.com/gabime/spdlog/v1.12.0.tar.gz",
            "https://gh-proxy.com/https://github.com/gabime/spdlog/archive/v1.12.0.tar.gz",
            "https://github.com/gabime/spdlog/archive/v1.12.0.tar.gz",
        ],
        patches = [
            Label("//third_party/patches/spdlog:change-namespace.patch"),
            Label("//third_party/patches/spdlog:change-rotating-file-sink.patch"),
        ],
        patch_args = ["-p1"],
        build_file = Label("//third_party:ds-spdlog.BUILD"),
    )

def setup_tbb(name = "tbb"):
    """Setup tbb library for Bazel builds."""
    maybe(
        http_archive,
        name = name,
        sha256 = "ebc4f6aa47972daed1f7bf71d100ae5bf6931c2e3144cf299c8cc7d041dca2f3",
        strip_prefix = "oneTBB-2020.3",
        urls = [
            "https://openyuanrong.obs.cn-southwest-2.myhuaweicloud.com/thirdparty/github.com/oneapi-src/oneTBB/v2020.3.tar.gz",
            "https://gh-proxy.com/https://github.com/uxlfoundation/oneTBB/archive/v2020.3.tar.gz",
            "https://github.com/uxlfoundation/oneTBB/archive/v2020.3.tar.gz",
        ],
        patches = [
            Label("//third_party/patches/tbb:2020.3/soft-link.patch"),
            Label("//third_party/patches/tbb:2020.3/adapt-task.h-to-gcc-14.patch"),
        ],
        patch_args = ["-p1"],
        build_file = Label("//third_party:tbb.BUILD"),
    )

def setup_zmq(name = "zmq", datasystem_repository = None):
    """Setup zmq library for Bazel builds."""
    repo_mapping = {}
    if datasystem_repository:
        repo_mapping["@yuanrong-datasystem"] = "@" + datasystem_repository
    maybe(
        http_archive,
        name = name,
        sha256 = "6c972d1e6a91a0ecd79c3236f04cf0126f2f4dfbbad407d72b4606a7ba93f9c6",
        strip_prefix = "libzmq-4.3.5",
        urls = [
            "https://gh-proxy.com/https://github.com/zeromq/libzmq/archive/v4.3.5.tar.gz",
            "https://github.com/zeromq/libzmq/archive/v4.3.5.tar.gz",
        ],
        build_file = Label("//third_party:zmq.BUILD"),
        repo_mapping = repo_mapping,
    )

def setup_jemalloc(name = "jemalloc_kvc"):
    """Setup Jemalloc library for Bazel builds."""
    maybe(
        http_archive,
        name = name,
        sha256 = "2db82d1e7119df3e71b7640219b6dfe84789bc0537983c3b7ac4f7189aecfeaa",
        strip_prefix = "jemalloc-5.3.0",
        urls = [
            "https://openyuanrong.obs.cn-southwest-2.myhuaweicloud.com/thirdparty/github.com/jemalloc/jemalloc/jemalloc-5.3.0.tar.bz2",
            "https://gh-proxy.com/https://github.com/jemalloc/jemalloc/releases/download/5.3.0/jemalloc-5.3.0.tar.bz2",
            "https://github.com/jemalloc/jemalloc/releases/download/5.3.0/jemalloc-5.3.0.tar.bz2",
        ],
        build_file = Label("//third_party:jemalloc.BUILD"),
    )

def setup_nlohmann_json(name = "nlohmann_json"):
    maybe(
        http_archive,
        name = name,
        sha256 = "0d8ef5af7f9794e3263480193c491549b2ba6cc74bb018906202ada498a79406",
        strip_prefix = "json-3.11.3",
        urls = [
            "https://gh-proxy.com/https://github.com/nlohmann/json/archive/v3.11.3.tar.gz",
            "https://github.com/nlohmann/json/archive/v3.11.3.tar.gz",
        ],
    )

def setup_bazel_skylib(name = "bazel_skylib"):
    maybe(
        http_archive,
        name = name,
        sha256 = "bc283cdfcd526a52c3201279cda4bc298652efa898b10b4db0837dc51652756f",
        urls = [
            "https://gh-proxy.com/https://github.com/bazelbuild/bazel-skylib/releases/download/1.7.1/bazel-skylib-1.7.1.tar.gz",
            "https://github.com/bazelbuild/bazel-skylib/releases/download/1.7.1/bazel-skylib-1.7.1.tar.gz",
        ],
    )

def setup_rocksdb(name = "rocksdb", bazel_skylib_repository = "bazel_skylib"):
    maybe(
        http_archive,
        name = name,
        sha256 = "4619ae7308cd3d11cdd36f0bfad3fb03a1ad399ca333f192b77b6b95b08e2f78",
        strip_prefix = "rocksdb-7.10.2",
        urls = [
            "https://gh-proxy.com/https://github.com/facebook/rocksdb/archive/refs/tags/v7.10.2.tar.gz",
            "https://github.com/facebook/rocksdb/archive/refs/tags/v7.10.2.tar.gz",
        ],
        patches = [Label("//third_party/patches/rocksdb:include-algorithm-for-gcc-14.patch")],
        patch_args = ["-p1"],
        build_file = Label("//third_party:rocksdb.BUILD"),
        repo_mapping = {
            "@bazel_skylib": "@" + bazel_skylib_repository,
        },
    )

def setup_gflags(name = "com_github_gflags_gflags"):
    """Setup gflags library for Bazel builds (brpc dependency)."""
    maybe(
        http_archive,
        name = name,
        sha256 = "34af2f15cf7367513b352bdcd2493ab14ce43692d2dcd9dfc499492966c64dcf",
        strip_prefix = "gflags-2.2.2",
        urls = [
            "https://gh-proxy.com/https://github.com/gflags/gflags/archive/refs/tags/v2.2.2.tar.gz",
            "https://github.com/gflags/gflags/archive/refs/tags/v2.2.2.tar.gz",
        ],
    )

def setup_leveldb(name = "com_github_google_leveldb"):
    """Setup leveldb library for Bazel builds (brpc dependency)."""
    maybe(
        http_archive,
        name = name,
        sha256 = "9a37f8a6174f09bd622bc723b55881dc541cd50747cbd08831c2a82d620f6d76",
        strip_prefix = "leveldb-1.23",
        urls = [
            "https://gh-proxy.com/https://github.com/google/leveldb/archive/refs/tags/1.23.tar.gz",
            "https://github.com/google/leveldb/archive/refs/tags/1.23.tar.gz",
        ],
        build_file = Label("//third_party:leveldb.BUILD"),
    )

def setup_brpc(
        name = "com_github_apache_brpc",
        gflags_repository = "com_github_gflags_gflags",
        leveldb_repository = "com_github_google_leveldb",
        avoid_glog_flag_conflicts = False):
    """Setup brpc library for Bazel builds."""
    patches = [Label("//third_party/patches/brpc:fix-boringssl-compat.patch")]
    if avoid_glog_flag_conflicts:
        patches.append(Label("//third_party/patches/brpc:avoid-glog-flag-conflicts.patch"))
    maybe(
        http_archive,
        name = name,
        sha256 = "f674b753af71dc313d9d2dcf34f574f0a3438c9f9bb9e7e6ca500a3b0ca7ddfb",
        urls = [
            "https://gh-proxy.com/https://github.com/apache/brpc/archive/refs/tags/1.15.0.tar.gz",
            "https://github.com/apache/brpc/archive/refs/tags/1.15.0.tar.gz",
        ],
        strip_prefix = "brpc-1.15.0",
        repo_mapping = {
            "@com_github_gflags_gflags": "@" + gflags_repository,
            "@com_github_google_leveldb": "@" + leveldb_repository,
            "@com_github_madler_zlib": "@zlib",
            "@openssl": "@boringssl",
        },
        patches = patches,
        patch_args = ["-p1"],
    )

def setup_braft(name = "braft"):
    """Setup braft using the existing brpc and common dependency repositories."""
    maybe(
        http_archive,
        name = name,
        sha256 = "bb3705f61874f8488e616ae38464efdec1a20610ddd6cd82468adc814488f14e",
        urls = [
            "https://gh-proxy.com/https://github.com/baidu/braft/archive/refs/tags/v1.1.2.tar.gz",
            "https://github.com/baidu/braft/archive/refs/tags/v1.1.2.tar.gz",
        ],
        strip_prefix = "braft-1.1.2",
        build_file = Label("//third_party:braft.BUILD"),
    )

def setup_curl(name = "curl", bazel_skylib_repository = "bazel_skylib"):
    maybe(
        http_archive,
        name = name,
        sha256 = "264537d90e58d2b09dddc50944baf3c38e7089151c8986715e2aaeaaf2b8118f",
        strip_prefix = "curl-8.11.0",
        urls = [
            "https://gh-proxy.com/https://github.com/curl/curl/releases/download/curl-8_11_0/curl-8.11.0.tar.gz",
            "https://github.com/curl/curl/releases/download/curl-8_11_0/curl-8.11.0.tar.gz",
        ],
        build_file = Label("//third_party:curl.BUILD"),
        repo_mapping = {
            "@bazel_skylib": "@" + bazel_skylib_repository,
        },
    )
