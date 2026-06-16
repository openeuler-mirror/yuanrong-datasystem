def _cuda_local_repository_impl(repository_ctx):
    """Expose a local CUDA Toolkit installation as a Bazel external repository.

    This follows the CMake CUDA discovery logic used by this repository:
      1. CUDA_HOME_PATH
      2. CUDA_CUSTOM_PATH
      3. CUDA_HOME
      4. /usr/local/cuda

    It also supports CUDA target-layout directories such as:
      /usr/local/cuda-12.8/targets/sbsa-linux
    """

    cuda_home_path = repository_ctx.getenv("CUDA_HOME_PATH")
    cuda_custom_path = repository_ctx.getenv("CUDA_CUSTOM_PATH")
    cuda_home = repository_ctx.getenv("CUDA_HOME")

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

        # Enable MOCK when build with no cuda headers
        build_content = """
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
        repository_ctx.file("BUILD.bazel", build_content)
        return

    libcudart = None
    for path in [
        cuda_root + "/lib64/libcudart.so",
        cuda_root + "/lib/libcudart.so",
    ]:
        if repository_ctx.path(path).exists:
            libcudart = path
            break

    repository_ctx.symlink(repository_ctx.path(cuda_root + "/include"), "include")

    build_content = """
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

    if libcudart != None:
        repository_ctx.symlink(repository_ctx.path(libcudart), "libcudart.so")
        build_content += """

cc_library(
    name = "cuda_runtime",
    hdrs = glob([
        "include/**/*.h",
        "include/**/*.hpp",
    ]),
    srcs = [
        "libcudart.so",
    ],
    includes = ["include"],
)
"""
    else:
        build_content += """

# Header-only fallback. Some targets, such as os_transport_pipeline, only need
# CUDA types and dynamically load libcudart.so at runtime through dlopen().
alias(
    name = "cuda_runtime",
    actual = ":cuda_headers",
)
"""

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
