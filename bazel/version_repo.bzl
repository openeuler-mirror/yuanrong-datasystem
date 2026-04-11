def _version_repo_impl(repository_ctx):
    version_file = repository_ctx.path(repository_ctx.attr.version_file)
    version = repository_ctx.read(version_file).strip()
    repository_ctx.file("BUILD.bazel", "")
    content = 'DATASYSTEM_VERSION = "{}"\n'.format(version)
    repository_ctx.file("version.bzl", content)

version_repo = repository_rule(
    implementation = _version_repo_impl,
    attrs = {
        "version_file": attr.label(default = "//:VERSION"),
    },
)