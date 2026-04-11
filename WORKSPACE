workspace(name = "yuanrong-datasystem")

load("//bazel:ds_deps.bzl", "ds_deps", "setup_grpc")
load("//bazel:version_repo.bzl", "version_repo")

version_repo(
    name = "datasystem_version",
    version_file = "//:VERSION",
)

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
