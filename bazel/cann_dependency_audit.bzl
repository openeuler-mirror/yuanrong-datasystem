"""Post-link ELF checks for the optional HIXL plugin boundary."""

def cann_dependency_audit(name, core_srcs, plugin_srcs, visibility = None):
    commands = ["set -euo pipefail"]
    for core in core_srcs:
        commands.append("""
for core_file in $(locations {core}); do
  if readelf -d "$$core_file" | grep -Eq 'Shared library: [[]lib(ascendcl|cann_hixl|metadef)[.]so'; then
    echo "Core artifact directly depends on a CANN library: $$core_file" >&2
    exit 1
  fi
  if nm -D -C --undefined-only "$$core_file" | grep -Eq 'hixl::|ge::AscendString'; then
    echo "Core artifact contains an undefined CANN C++ ABI symbol: $$core_file" >&2
    exit 1
  fi
done
""".format(core = core))
    for plugin in plugin_srcs:
        commands.append("""
for plugin_file in $(locations {plugin}); do
  for required_lib in libascendcl.so libcann_hixl.so libmetadef.so; do
    if ! readelf -d "$$plugin_file" | grep -Fq "Shared library: [$$required_lib"; then
      echo "HIXL plugin is missing dependency $$required_lib: $$plugin_file" >&2
      exit 1
    fi
  done
  if ! readelf --dyn-syms --wide "$$plugin_file" | grep -Fq DsHixlGetApi; then
    echo "HIXL plugin does not export DsHixlGetApi: $$plugin_file" >&2
    exit 1
  fi
  extra_exports=$$(readelf --dyn-syms --wide "$$plugin_file" | awk '($$5 == "GLOBAL" || $$5 == "WEAK") && $$7 != "UND" {{print $$8}}' | grep -v '^DsHixlGetApi$$' || true)
  if [ -n "$$extra_exports" ]; then
    echo "HIXL plugin exposes unexpected global symbols: $$extra_exports" >&2
    exit 1
  fi
done
""".format(plugin = plugin))
    commands.append("touch $@")

    native.genrule(
        name = name,
        srcs = core_srcs + plugin_srcs,
        outs = [name + ".stamp"],
        cmd = "\n".join(commands),
        visibility = visibility,
    )
