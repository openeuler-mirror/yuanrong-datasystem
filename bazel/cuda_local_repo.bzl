"""CUDA Toolkit as @local_cuda, with an RPM-backed mode.

Three modes, tried in order (fall back on failure):
  1. RPM-backed CUDA from the NVIDIA rhel9 repo (default). The full dependency
     RPM list (obtained once with `yum install --downloadonly --downloaddir=./
     <pkgs>`) is hardcoded in _RPM_FILES; each rpm is downloaded with Bazel's
     own downloader (repository_ctx.download, the same path as every
     http_archive dep, so proxy/redirects behave like the rest of the build)
     into the repository root (execroot/.../external/local_cuda), with the
     baseurl selected by `uname -m` (aarch64 -> sbsa, everything else ->
     x86_64). Payloads are extracted in place without root (rpm2cpio + cpio);
     the rpms are kept, and a valid rpm already on disk is reused so local=True
     re-runs do not re-fetch.
  2. System CUDA discovery following the CMake discovery logic (CUDA_HOME_PATH
     / CUDA_CUSTOM_PATH / CUDA_HOME / /usr/local/cuda), including
     target-layout directories.
  3. A PIPLN_USE_MOCK stub.
"""

_ARCH_REPO = {
    "aarch64": "https://developer.download.nvidia.com/compute/cuda/repos/rhel9/sbsa",
    "x86_64": "https://developer.download.nvidia.com/compute/cuda/repos/rhel9/x86_64",
}

# Full dependency closure of cuda-cudart-devel-12-8-12.8.57-1 and
# cuda-nvcc-12-8-12.8.61-1 per `yum install --downloadonly`. {arch} is the host
# arch rpm suffix (aarch64/x86_64); noarch packages are shared between arches.
_RPM_FILES = [
    "cuda-cudart-devel-12-8-12.8.57-1.{arch}.rpm",
    "cuda-nvcc-12-8-12.8.61-1.{arch}.rpm",
    "cuda-cccl-12-8-12.8.90-1.{arch}.rpm",
    "cuda-crt-12-8-12.8.61-1.{arch}.rpm",
    "cuda-cudart-12-8-12.8.90-1.{arch}.rpm",
    "cuda-nvvm-12-8-12.8.61-1.{arch}.rpm",
    "cuda-toolkit-12-8-config-common-12.8.90-1.noarch.rpm",
    "cuda-toolkit-12-config-common-12.9.79-1.noarch.rpm",
    "cuda-toolkit-config-common-13.3.29-1.noarch.rpm",
]

def _find_file(repository_ctx, name):
    """Return the first path matching `name` under the repo root, or empty."""
    result = repository_ctx.execute(
        ["find", ".", "-name", name, "-type", "f", "-o", "-name", name, "-type", "l"],
        quiet = True,
    )
    if result.return_code != 0:
        return ""
    for line in result.stdout.strip().split("\n"):
        line = line.strip()
        if line:
            return line
    return ""

def _cuda_rpm_provide(repository_ctx):
    """Download + extract the CUDA RPM closure into the repository dir.

    Returns a struct with include_dir / lib_dir / lib_file / bin_dir (bin_dir
    is None when nvcc is absent), or None when the RPM path fails, so the caller
    falls through to system CUDA discovery.
    """
    uname = repository_ctx.execute(["uname", "-m"], quiet = True)
    arch = uname.stdout.strip()
    repo_base = _ARCH_REPO.get(arch)
    if repo_base == None:
        print("WARNING: cuda_rpm: unsupported arch '%s' (expected aarch64 or x86_64), falling back to system CUDA" % arch)
        return None

    # repository_ctx.download() uses Bazel's own downloader (the same path as
    # every http_archive dep in this workspace), so proxy env, redirects and the
    # repository cache all behave like the rest of the build. The rpms are kept
    # in the repo root (execroot/.../external/local_cuda); an rpm already on disk
    # that passes `rpm -K --nosignature` (digest check) is reused, so local=True
    # re-runs do not re-fetch and interrupted/corrupt files are re-downloaded.
    for name in _RPM_FILES:
        filename = name.format(arch = arch)
        valid = repository_ctx.execute(
            ["bash", "-c", "rpm -K --nosignature '%s' >/dev/null 2>&1" % filename],
            quiet = True,
        )
        if valid.return_code == 0:
            continue
        result = repository_ctx.download(
            repo_base + "/" + filename,
            output = filename,
            allow_fail = True,
        )
        if not result.success:
            print("WARNING: cuda_rpm: download failed: %s/%s, falling back to system CUDA" % (repo_base, filename))
            return None

    extract_script = """set -e
for rpm in *.rpm; do
    rpm2cpio "$rpm" | cpio -idmu --no-absolute-filenames --quiet
done
"""
    result = repository_ctx.execute(["bash", "-c", extract_script], quiet = False)
    if result.return_code != 0:
        print("WARNING: cuda_rpm: extraction failed, falling back to system CUDA")
        return None

    inc = _find_file(repository_ctx, "cuda_runtime.h")
    if inc == "":
        print("WARNING: cuda_rpm: cuda_runtime.h not found, falling back to system CUDA")
        return None
    lib = _find_file(repository_ctx, "libcudart.so")
    if lib == "":
        lib = _find_file(repository_ctx, "libcudart.so.12")
    if lib == "":
        print("WARNING: cuda_rpm: libcudart not found, falling back to system CUDA")
        return None
    nvcc = _find_file(repository_ctx, "nvcc")
    return struct(
        include_dir = inc.rsplit("/", 1)[0],
        lib_dir = lib.rsplit("/", 1)[0],
        lib_file = lib.rsplit("/", 1)[1],
        bin_dir = nvcc.rsplit("/", 1)[0] if nvcc != "" else None,
    )


_CUDA_HEADERS = """
package(default_visibility = ["//visibility:public"])

cc_library(
    name = "cuda_headers",
    hdrs = glob([
        "include/**/*.h",
        "include/**/*.hpp",
    ]),
    includes = ["include"],
)
"""

_CUDA_RUNTIME_SRC = """

cc_library(
    name = "cuda_runtime",
    hdrs = glob([
        "include/**/*.h",
        "include/**/*.hpp",
    ]),
    srcs = ["%s"],
    includes = ["include"],
)
"""

_CUDA_RUNTIME_ALIAS = """

# Header-only fallback. Some targets, such as os_transport_pipeline, only need
# CUDA types and dynamically load libcudart.so at runtime through dlopen().
alias(
    name = "cuda_runtime",
    actual = ":cuda_headers",
)
"""

_NVCC_TARGET = """
filegroup(
    name = "nvcc",
    srcs = ["bin/nvcc"],
)
"""

_MOCK_BUILD = """
package(default_visibility = ["//visibility:public"])

cc_library(
    name = "cuda_headers",
    defines = ["PIPLN_USE_MOCK"],
)

alias(
    name = "cuda_runtime",
    actual = ":cuda_headers",
)
"""

def _write_rpm_build(repository_ctx, rpm):
    """Symlink the extracted RPM tree and expose cuda_headers/cuda_runtime/nvcc."""
    for path in ["include", "lib", "bin"]:
        repository_ctx.delete(path)
    repository_ctx.symlink(repository_ctx.path(rpm.include_dir), "include")
    repository_ctx.symlink(repository_ctx.path(rpm.lib_dir), "lib")
    build_content = _CUDA_HEADERS + _CUDA_RUNTIME_SRC % ("lib/" + rpm.lib_file)
    if rpm.bin_dir != None:
        repository_ctx.symlink(repository_ctx.path(rpm.bin_dir), "bin")
        build_content += _NVCC_TARGET
    repository_ctx.file("BUILD.bazel", build_content)

def _cuda_local_repository_impl(repository_ctx):
    rpm = _cuda_rpm_provide(repository_ctx)
    if rpm != None:
        _write_rpm_build(repository_ctx, rpm)
        return

    cuda_home_path = repository_ctx.os.environ.get("CUDA_HOME_PATH")
    cuda_custom_path = repository_ctx.os.environ.get("CUDA_CUSTOM_PATH")
    cuda_home = repository_ctx.os.environ.get("CUDA_HOME")

    candidates = []
    for path in [
        cuda_home_path,
        cuda_custom_path,
        cuda_home,
        "/usr/local/cuda",
        "/usr/local/cuda-12.8",
    ]:
        if path:
            candidates.append(path)
            candidates.append(path + "/targets/sbsa-linux")
            candidates.append(path + "/targets/aarch64-linux")
            candidates.append(path + "/targets/x86_64-linux")

    cuda_root = None
    for path in candidates:
        if repository_ctx.path(path + "/include/cuda.h").exists and repository_ctx.path(path + "/include/cuda_runtime.h").exists:
            cuda_root = path
            break

    if cuda_root == None:
        print("WARNING: Cannot find CUDA Toolkit headers in the current environment.")
        print("WARNING: Fallback mode enabled. The 'PIPLN_USE_MOCK' macro will be defined.")
        print("""
Tried:
  CUDA_HOME_PATH
  CUDA_CUSTOM_PATH
  CUDA_HOME
  /usr/local/cuda
  /usr/local/cuda-12.8
  */targets/sbsa-linux
  */targets/aarch64-linux
  */targets/x86_64-linux

Expected:
  ${CUDA_ROOT}/include/cuda.h
  ${CUDA_ROOT}/include/cuda_runtime.h

For your current environment, you can set:
  export CUDA_CUSTOM_PATH=/usr/local/cuda-12.8/targets/sbsa-linux
""")
        repository_ctx.file("BUILD.bazel", _MOCK_BUILD)
        return

    libcudart = None
    for path in [
        cuda_root + "/lib64/libcudart.so",
        cuda_root + "/lib/libcudart.so",
    ]:
        if repository_ctx.path(path).exists:
            libcudart = path
            break

    repository_ctx.delete("include")
    repository_ctx.symlink(repository_ctx.path(cuda_root + "/include"), "include")

    build_content = _CUDA_HEADERS
    if libcudart != None:
        repository_ctx.delete("libcudart.so")
        repository_ctx.symlink(repository_ctx.path(libcudart), "libcudart.so")
        build_content += _CUDA_RUNTIME_SRC % "libcudart.so"
    else:
        build_content += _CUDA_RUNTIME_ALIAS
    repository_ctx.file("BUILD.bazel", build_content)


cuda_local_repository = repository_rule(
    implementation = _cuda_local_repository_impl,
    local = True,
    environ = [
        "CUDA_HOME_PATH",
        "CUDA_CUSTOM_PATH",
        "CUDA_HOME",
    ],
)
