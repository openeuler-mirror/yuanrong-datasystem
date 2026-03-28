workspace(name = "yuanrong-datasystem")

load("//third_party/bazel:setup_all.bzl", "setup_all_third_party_libraries")

setup_all_third_party_libraries()

#load("//:version.bzl", "DATASYSTEM_VERSION")
load("//bazel:glibc_detect.bzl", "glibc_detect")
load("//bazel:python_detect.bzl", "python_detect")

glibc_detect(name = "local_glibc_info")
python_detect(name = "local_python_info")