"""Bazel 6 WORKSPACE entrypoint for source consumers of the DataSystem SDK."""

load("//bazel/sdk:deps.bzl", "source_sdk_deps")

_SOURCE_SDK_EXCLUDED_ENTRIES = {
    ".git": True,
    "BUILD": True,
    "BUILD.bazel": True,
    "WORKSPACE": True,
    "WORKSPACE.bazel": True,
    "bazel-bin": True,
    "bazel-out": True,
    "bazel-testlogs": True,
    "build": True,
    "output": True,
}


def _datasystem_source_repository_impl(repository_ctx):
    if not repository_ctx.attr.enabled:
        repository_ctx.file("WORKSPACE", "workspace(name = \"%s\")\n" % repository_ctx.name)
        repository_ctx.file("BUILD.bazel", "")
        repository_ctx.file(
            "bazel/sdk/BUILD.bazel",
            repository_ctx.read(repository_ctx.attr.stub_build_file),
        )
        return

    source_path = repository_ctx.attr.path
    if source_path[0] != "/":
        workspace_root = repository_ctx.path(Label("@//:BUILD.bazel")).dirname
        source_path = repository_ctx.path(str(workspace_root) + "/" + source_path)
    else:
        source_path = repository_ctx.path(source_path)

    if not source_path.exists:
        fail("DataSystem source path does not exist: %s" % source_path)

    repository_ctx.file("WORKSPACE", "workspace(name = \"%s\")\n" % repository_ctx.name)
    for entry in source_path.readdir():
        entry_name = entry.basename
        if entry_name in _SOURCE_SDK_EXCLUDED_ENTRIES or entry_name.startswith("bazel-"):
            continue
        repository_ctx.symlink(entry, repository_ctx.path(entry_name))

    native_build = repository_ctx.path(repository_ctx.attr.native_build_path)
    if not native_build.exists:
        fail("DataSystem SDK root BUILD file does not exist: %s" % native_build)
    repository_ctx.symlink(native_build, repository_ctx.path("BUILD.bazel"))


_datasystem_source_repository = repository_rule(
    implementation = _datasystem_source_repository_impl,
    attrs = {
        "enabled": attr.bool(mandatory = True),
        "native_build_path": attr.string(mandatory = True),
        "path": attr.string(mandatory = True),
        "stub_build_file": attr.label(allow_single_file = True, mandatory = True),
    },
    local = True,
)


def datasystem_source_sdk(
        name,
        path,
        enabled,
        re2_repository = "com_googlesource_code_re2"):
    """Registers the DataSystem source SDK and its private dependency graph."""
    _datasystem_source_repository(
        name = name,
        enabled = enabled,
        native_build_path = "bazel/sdk/root.BUILD.bazel",
        path = path,
        repo_mapping = {
            "@com_github_apache_brpc": "@ds_brpc",
            "@com_github_gflags_gflags": "@ds_gflags",
            "@com_github_google_leveldb": "@ds_leveldb",
            "@curl": "@ds_libcurl",
            "@ds-spdlog": "@ds_spdlog",
            "@jemalloc_kvc": "@ds_jemalloc",
            "@nlohmann_json": "@ds_nlohmann_json",
            "@re2": "@" + re2_repository,
            "@rocksdb": "@ds_rocksdb",
            "@tbb": "@ds_tbb",
            "@yuanrong-datasystem": "@" + name,
            "@zmq": "@ds_libzmq",
        },
        stub_build_file = Label("//bazel/sdk:stub.BUILD.bazel"),
    )
    if enabled:
        source_sdk_deps(datasystem_repository = name)
