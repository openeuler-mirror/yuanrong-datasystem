"""Build-graph validation for the public source SDK target."""


def _source_sdk_validation_impl(ctx):
    marker = ctx.actions.declare_file(ctx.label.name + ".marker")
    target_files = ctx.attr.target[DefaultInfo].files
    ctx.actions.run_shell(
        inputs = target_files,
        outputs = [marker],
        command = "touch %s" % marker.path,
        mnemonic = "SourceSdkValidation",
        progress_message = "Validating DataSystem source SDK client",
    )
    return [
        DefaultInfo(),
        PyInfo(transitive_sources = depset()),
        OutputGroupInfo(_validation = depset([marker])),
    ]


source_sdk_validation = rule(
    implementation = _source_sdk_validation_impl,
    attrs = {
        "target": attr.label(mandatory = True),
    },
)
