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

load("@rules_python//python:repositories.bzl", "python_register_toolchains")

python_register_toolchains(
    name = "python_3_9",
    python_version = "3.9",
    ignore_root_user_error= True
)

python_register_toolchains(
    name = "python_3_10",
    python_version = "3.10",
    ignore_root_user_error= True
)

python_register_toolchains(
    name = "python_3_11",
    python_version = "3.11",
    ignore_root_user_error= True
)

python_register_toolchains(
    name = "python_3_12",
    python_version = "3.12",
    ignore_root_user_error= True
)

python_register_toolchains(
    name = "python_3_13",
    python_version = "3.13",
    ignore_root_user_error= True
)
load("@python_3_9//:defs.bzl", "interpreter")
load("@python_3_10//:defs.bzl", "interpreter")
load("@python_3_11//:defs.bzl", "interpreter")
load("@python_3_12//:defs.bzl", "interpreter")
load("@python_3_13//:defs.bzl", "interpreter")
