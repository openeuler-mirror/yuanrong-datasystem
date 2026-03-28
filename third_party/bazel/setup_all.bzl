# Main entry point for third-party library setup

"""
This file provides a unified interface to set up all third-party libraries
required by the DataSystem project.
"""

load("//third_party/bazel:absl.bzl", "setup_absl")
load("//third_party/bazel:gtest.bzl", "setup_gtest")
load("//third_party/bazel:jemalloc.bzl", "setup_jemalloc")
load("//third_party/bazel:nlohmann_json.bzl", "setup_nlohmann_json")
load("//third_party/bazel:openssl.bzl", "setup_openssl")
load("//third_party/bazel:preload_grpc.bzl", "preload_grpc")
load("//third_party/bazel:protobuf.bzl", "setup_protobuf")
load("//third_party/bazel:re2.bzl", "setup_re2")
load("//third_party/bazel:securec.bzl", "setup_securec")
load("//third_party/bazel:spdlog.bzl", "setup_spdlog")
load("//third_party/bazel:tbb.bzl", "setup_tbb")
load("//third_party/bazel:zmq.bzl", "setup_zmq")

def setup_all_third_party_libraries():
    """Set up all third-party libraries required by the project."""

    # Supports Bazel compilation
    setup_jemalloc()
    #preload_grpc()
    #setup_gtest()

    #setup_absl()
    #setup_nlohmann_json()
    #setup_openssl()
    #setup_protobuf()

    # Bazel compilation is not supported.
    setup_securec()

    setup_spdlog()
    setup_tbb()
    setup_zmq()

    # With orther deps.
    #setup_re2()
