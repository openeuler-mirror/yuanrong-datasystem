def _datasystem_header_tree_impl(ctx):
    out_dir = ctx.actions.declare_directory(ctx.attr.out)
    args = ctx.actions.args()
    args.add(out_dir.path)
    prefix = "include/datasystem/"
    for header in ctx.files.headers:
        args.add(header.path)
        args.add(header.short_path[len(prefix):])

    ctx.actions.run_shell(
        inputs = ctx.files.headers,
        outputs = [out_dir],
        arguments = [args],
        command = """
set -eu
out_dir="$1"
shift
mkdir -p "$out_dir"
while [ "$#" -gt 0 ]; do
    src="$1"
    rel="$2"
    shift 2
    mkdir -p "$out_dir/$(dirname "$rel")"
    cp -f "$src" "$out_dir/$rel"
done
""",
    )
    return [DefaultInfo(
        files = depset([out_dir]),
        runfiles = ctx.runfiles(files = [out_dir]),
    )]

datasystem_header_tree = rule(
    implementation = _datasystem_header_tree_impl,
    attrs = {
        "headers": attr.label_list(allow_files = True, mandatory = True),
        "out": attr.string(mandatory = True),
    },
)
