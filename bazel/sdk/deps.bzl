"""Optional repository configuration required by the source SDK targets."""

load("@bazel_tools//tools/build_defs/repo:utils.bzl", "maybe")
load("//bazel:ascend_configure.bzl", "ascend_configure")
load("//bazel:cuda_local_repo.bzl", "cuda_local_repository")
load(
    "//bazel/sdk:repositories.bzl",
    "setup_brpc",
    "setup_bazel_skylib",
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

def source_sdk_deps(datasystem_repository = "datasystem_sdk"):
    maybe(ascend_configure, name = "local_ascend")
    maybe(cuda_local_repository, name = "local_cuda")
    setup_spdlog(name = "ds_spdlog")
    setup_tbb(name = "ds_tbb")
    setup_zmq(name = "ds_libzmq", datasystem_repository = datasystem_repository)
    setup_jemalloc(name = "ds_jemalloc")
    setup_bazel_skylib(name = "ds_bazel_skylib")
    setup_rocksdb(name = "ds_rocksdb", bazel_skylib_repository = "ds_bazel_skylib")
    setup_curl(name = "ds_libcurl", bazel_skylib_repository = "ds_bazel_skylib")
    setup_gflags(name = "ds_gflags")
    setup_leveldb(name = "ds_leveldb")
    setup_nlohmann_json(name = "ds_nlohmann_json")
    setup_brpc(
        name = "ds_brpc",
        gflags_repository = "ds_gflags",
        leveldb_repository = "ds_leveldb",
        avoid_glog_flag_conflicts = True,
    )
