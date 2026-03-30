workspace(name = "yuanrong-datasystem")

load("//:version.bzl", "DATASYSTEM_VERSION")
load("//bazel:ds_deps.bzl", "ds_deps", "setup_grpc")

ds_deps()

load("@com_google_googleapis//:repository_rules.bzl", "switched_rules_by_language")

switched_rules_by_language(
    name = "com_google_googleapis_imports",
    cc = True,
    go = False,
    grpc = True,
    java = False,
    python = False,
)

setup_grpc()

load("//bazel:glibc_detect.bzl", "glibc_detect")
load("//bazel:python_detect.bzl", "python_detect")

glibc_detect(name = "local_glibc_info")

python_detect(name = "local_python_info")

load("@com_github_grpc_grpc//third_party/py:python_configure.bzl", "python_configure")

python_configure(name = "local_config_python")

load("@rules_python//python:repositories.bzl", "py_repositories")

py_repositories()
