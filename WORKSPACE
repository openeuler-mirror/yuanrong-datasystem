workspace(name = "yuanrong-datasystem")

load("//:version.bzl", "DATASYSTEM_VERSION")
load("//bazel:ds_deps.bzl", "ds_deps", "setup_grpc")
load("//bazel:ascend_configure.bzl", "ascend_configure")

ds_deps()

ascend_configure(name = "local_ascend")

load("//bazel/sdk:workspace.bzl", "datasystem_source_sdk")

# Validate the public source SDK through the same build graph used by the
# wheel. This repository is private to the build and does not change package
# contents or add a separate CI entrypoint.
datasystem_source_sdk(
    name = "datasystem_sdk_validation",
    path = ".",
    enabled = True,
    re2_repository = "re2",
)

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

load("@com_github_grpc_grpc//third_party/py:python_configure.bzl", "python_configure")

python_configure(name = "local_config_python")

load("@rules_python//python:repositories.bzl", "py_repositories", "python_register_toolchains")
load("//bazel:glibc_detect.bzl", "glibc_detect")
load("//bazel:python_detect.bzl", "python_detect")

py_repositories()

glibc_detect(name = "local_glibc_info")

python_detect(name = "local_python_info")

load("@local_python_info//:version.bzl", "PYTHON_VERSION")

# Use system python version default
python_register_toolchains(
    name = "python_system_detected",
    ignore_root_user_error = True,
    python_version = PYTHON_VERSION,
)

python_register_toolchains(
    name = "python_3_9",
    ignore_root_user_error = True,
    python_version = "3.9",
)

python_register_toolchains(
    name = "python_3_10",
    ignore_root_user_error = True,
    python_version = "3.10",
)

python_register_toolchains(
    name = "python_3_11",
    ignore_root_user_error = True,
    python_version = "3.11",
)

python_register_toolchains(
    name = "python_3_12",
    ignore_root_user_error = True,
    python_version = "3.12",
)

python_register_toolchains(
    name = "python_3_13",
    ignore_root_user_error = True,
    python_version = "3.13",
)
