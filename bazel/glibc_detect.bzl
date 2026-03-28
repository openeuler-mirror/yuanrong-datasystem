def _glibc_detect_impl(repository_ctx):
    
    result = repository_ctx.execute(["ldd", "--version"])
    detected_version = "2.34"
    
    if result.return_code == 0:
        first_line = result.stdout.strip().splitlines()[0]
        parts = first_line.split(" ")
        if parts:
            raw_version = parts[-1]
            ver_parts = raw_version.split(".")
            if len(ver_parts) >= 2:
                detected_version = "{}.{}".format(ver_parts[0], ver_parts[1])
    
    arch_result = repository_ctx.execute(["uname", "-m"])
    detected_arch = "x86_64"
    
    if arch_result.return_code == 0:
        machine = arch_result.stdout.strip()
        if machine in ["aarch64", "arm64"]:
            detected_arch = "aarch64"
    
    version_fmt = detected_version.replace(".", "_")
    
    supported_versions = ["2.34", "2.35", "2.36", "2.37", "2.38"]
    
    platform_lines = []
    for v in supported_versions:
        v_fmt = v.replace(".", "_")
        platform_lines.append('GLIBC_{}_PLATFORM = "manylinux_{}_{}"'.format(
            v_fmt, v_fmt, detected_arch))
    
    repository_ctx.file("version.bzl", content = '''# Auto-generated GLIBC detection
# Detected version: {detected_version}
# Detected architecture: {detected_arch}

SYSTEM_GLIBC_VERSION = "{detected_version}"
SYSTEM_GLIBC_ARCH = "{detected_arch}"

AUTO_GLIBC_PLATFORM = "manylinux_{version_fmt}_{detected_arch}"

{platform_lines}
'''.format(
        detected_version = detected_version,
        detected_arch = detected_arch,
        version_fmt = version_fmt,
        platform_lines = "\n".join(platform_lines),
    ))
    
    repository_ctx.file("BUILD.bazel", "")

glibc_detect = repository_rule(
    implementation = _glibc_detect_impl,
    local = True,
    environ = ["PATH"],
)