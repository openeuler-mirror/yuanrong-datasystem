def _datasystem_sdk_tree_impl(ctx):
    out_dir = ctx.actions.declare_directory(ctx.label.name)
    headers_manifest = ctx.actions.declare_file(ctx.label.name + "_headers_manifest.txt")

    header_lines = []
    prefix = "include/datasystem/"
    for header in sorted(ctx.files.headers, key = lambda f: f.short_path):
        if not header.short_path.startswith(prefix):
            fail("Header %s is not under include/datasystem/" % header.short_path)
        rel = header.short_path[len(prefix):]
        header_lines.append("%s\t%s" % (header.path, rel))

    ctx.actions.write(
        output = headers_manifest,
        content = "\n".join(header_lines) + "\n",
    )

    command = """
set -euo pipefail

out_dir="$1"
headers_manifest="$2"
build_tpl="$3"
libdatasystem="$4"
libds_client_py="$5"

mkdir -p "$out_dir/cpp/lib"
cp -f "$build_tpl" "$out_dir/cpp/BUILD.bazel"
cp -f "$libdatasystem" "$out_dir/cpp/lib/libdatasystem.so"
cp -f "$libds_client_py" "$out_dir/cpp/lib/libds_client_py.so"

while IFS=$'\\t' read -r src rel; do
  if [[ -z "$src" || -z "$rel" ]]; then
    continue
  fi

  mkdir -p "$out_dir/cpp/include/datasystem/$(dirname "$rel")"
  cp -f "$src" "$out_dir/cpp/include/datasystem/$rel"
done < "$headers_manifest"
"""

    ctx.actions.run_shell(
        inputs = depset(
            [headers_manifest, ctx.file.build_tpl, ctx.file.libdatasystem, ctx.file.libds_client_py],
            transitive = [depset(ctx.files.headers)],
        ),
        outputs = [out_dir],
        command = command,
        arguments = [
            out_dir.path,
            headers_manifest.path,
            ctx.file.build_tpl.path,
            ctx.file.libdatasystem.path,
            ctx.file.libds_client_py.path,
        ],
        mnemonic = "BuildDatasystemSdkTree",
        progress_message = "Packaging datasystem SDK tree for %s" % ctx.label,
    )

    return [DefaultInfo(files = depset([out_dir]))]

datasystem_sdk_tree = rule(
    implementation = _datasystem_sdk_tree_impl,
    attrs = {
        "headers": attr.label_list(allow_files = True, mandatory = True),
        "build_tpl": attr.label(allow_single_file = True, mandatory = True),
        "libdatasystem": attr.label(allow_single_file = True, mandatory = True),
        "libds_client_py": attr.label(allow_single_file = True, mandatory = True),
    },
)
