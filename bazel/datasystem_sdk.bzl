def _datasystem_sdk_tree_impl(ctx):
    out_dir = ctx.actions.declare_directory(ctx.label.name)
    out_tar = ctx.actions.declare_file(ctx.label.name + ".tar")
    header_args = []

    prefix = "include/datasystem/"
    for header in sorted(ctx.files.headers, key = lambda f: f.short_path):
        if not header.short_path.startswith(prefix):
            fail("Header %s is not under include/datasystem/" % header.short_path)
        rel = header.short_path[len(prefix):]
        header_args.append(header.path)
        header_args.append(rel)

    command = """
set -euo pipefail

out_dir="$1"
build_tpl="$2"
libdatasystem="$3"
out_tar="$4"
shift 4

mkdir -p "$out_dir/cpp/lib"
cp -f "$build_tpl" "$out_dir/cpp/BUILD.bazel"
cp -f "$libdatasystem" "$out_dir/cpp/lib/libdatasystem.so"

while [ "$#" -ge 2 ]; do
  src="$1"
  rel="$2"
  shift 2
  mkdir -p "$out_dir/cpp/include/datasystem/$(dirname "$rel")"
  cp -f "$src" "$out_dir/cpp/include/datasystem/$rel"
done

if [ "$#" -ne 0 ]; then
  echo "Invalid header arg list." >&2
  exit 1
fi

parent_dir="$(dirname "$out_dir")"
base_name="$(basename "$out_dir")"
tar_name="$(basename "$out_tar")"
(
  cd "$parent_dir"
  rm -f "$tar_name"
  tar -cf "$tar_name" "$base_name"
)
"""

    ctx.actions.run_shell(
        inputs = depset(
            [ctx.file.build_tpl, ctx.file.libdatasystem],
            transitive = [depset(ctx.files.headers)],
        ),
        outputs = [out_dir, out_tar],
        command = command,
        arguments = [
            out_dir.path,
            ctx.file.build_tpl.path,
            ctx.file.libdatasystem.path,
            out_tar.path,
        ] + header_args,
        mnemonic = "BuildDatasystemSdkTree",
        progress_message = "Packaging datasystem SDK tree for %s" % ctx.label,
    )

    return [DefaultInfo(files = depset([out_dir, out_tar]))]

datasystem_sdk_tree = rule(
    implementation = _datasystem_sdk_tree_impl,
    attrs = {
        "headers": attr.label_list(allow_files = True, mandatory = True),
        "build_tpl": attr.label(allow_single_file = True, mandatory = True),
        "libdatasystem": attr.label(allow_single_file = True, mandatory = True),
    },
)
