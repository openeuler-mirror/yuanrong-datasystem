"""URMA SDK external repository.

Two modes controlled by the URMA_PKG_URL environment variable:
  - URMA_PKG_URL set    : download the package, auto-locate headers/libs,
                          restructure headers into ub/umdk/urma/ if needed
  - URMA_PKG_URL empty  : use system-installed URMA SDK (/usr/include, /usr/lib64)

The URMA_PKG_URL / URMA_PKG_SHA256 values are injected via .bazelrc:
    build --repo_env=URMA_PKG_URL=<url>
    build --repo_env=URMA_PKG_SHA256=<sha256>
Comment them out to fall back to the system-installed URMA SDK.

The package archive type is auto-detected from the URL file extension, so
.zip, .tar.gz, .tar.bz2 and .tar.xz are all supported. This matches the
CMake download path (cmake/external_libs/urma.cmake -> FetchContent_Populate),
which also auto-detects the archive type. Keep the URL ending with one of
these extensions; URLs without a recognizable extension are not supported.

Modeled after bazel/cuda_local_repo.bzl.
"""

def _find_file_via_shell(repository_ctx, root, filename):
    """Find a file by name under root using shell 'find'.

    Returns the path string relative to the repo root, or empty string.
    Searches for regular files and symlinks.
    """
    result = repository_ctx.execute(
        ["find", root, "-name", filename, "-type", "f", "-o", "-name", filename, "-type", "l"],
        quiet = True,
    )
    if result.return_code != 0:
        return ""
    lines = result.stdout.strip().split("\n")
    for line in lines:
        line = line.strip()
        if line:
            return line
    return ""

def _find_all_files_via_shell(repository_ctx, root, basenames):
    """Find all files matching any of the basenames under root.

    Returns a list of path strings. Searches for regular files and symlinks.
    """
    result = []
    for name in basenames:
        find_result = repository_ctx.execute(
            ["find", root, "-name", name, "-type", "f", "-o", "-name", name, "-type", "l"],
            quiet = True,
        )
        if find_result.return_code == 0:
            for line in find_result.stdout.strip().split("\n"):
                line = line.strip()
                if line:
                    result.append(line)
    return result

def _urma_pkg_repository_impl(repository_ctx):
    urma_pkg_url = repository_ctx.getenv("URMA_PKG_URL", "")
    urma_pkg_sha256 = repository_ctx.getenv("URMA_PKG_SHA256", "")

    if urma_pkg_url != "":
        # --- Download package ---
        if urma_pkg_sha256 == "":
            fail("URMA_PKG_SHA256 must be set when URMA_PKG_URL is set "
                 + "(downloading without checksum is unsafe)")
        repository_ctx.download_and_extract(
            urma_pkg_url,
            sha256 = urma_pkg_sha256,
        )

        # Auto-locate include root: find urma_api.h anchor.
        # The package may ship headers flat (urma_api.h in some include/ dir)
        # or already under ub/umdk/urma/. If flat, restructure into
        # ub/umdk/urma/ to match #include <ub/umdk/urma/*.h>.
        urma_api_path = _find_file_via_shell(repository_ctx, ".", "urma_api.h")
        if urma_api_path == "":
            fail("urma_api.h not found in downloaded URMA package")

        # Determine the include source directory (directory containing urma_api.h)
        # and check if headers are already under ub/umdk/urma/.
        if "/ub/umdk/urma/" in urma_api_path:
            # Headers already in ub/umdk/urma/ — include root is the parent
            # of ub/. Symlink the include dir as-is.
            # e.g. path = "rootfs/usr/include/ub/umdk/urma/urma_api.h"
            # include root = "rootfs/usr/include"
            parts = urma_api_path.split("/ub/umdk/urma/")
            include_root = parts[0]
            repository_ctx.symlink(repository_ctx.path(include_root), "include")
        else:
            # Headers are flat — restructure into ub/umdk/urma/.
            # include_src_dir = directory containing urma_api.h
            path_parts = urma_api_path.split("/")
            include_src_dir = "/".join(path_parts[:-1])

            # Find all .h files in the same directory and symlink them
            find_h_result = repository_ctx.execute(
                ["find", include_src_dir, "-name", "*.h", "-type", "f"],
                quiet = True,
            )
            for line in find_h_result.stdout.strip().split("\n"):
                line = line.strip()
                if line:
                    basename = line.split("/")[-1]
                    repository_ctx.symlink(repository_ctx.path(line), "ub/umdk/urma/" + basename)

        # Auto-locate lib root: find liburma.so anchor.
        urma_so_path = _find_file_via_shell(repository_ctx, ".", "liburma.so")
        if urma_so_path == "":
            fail("liburma.so not found in downloaded URMA package")

        # lib_dir = directory containing liburma.so
        lib_path_parts = urma_so_path.split("/")
        lib_dir = "/".join(lib_path_parts[:-1])

        # Find all 6 urma so files (and .so.0 versions) and symlink into lib/.
        # If .so.0 is missing, symlink .so.0 -> .so so all 12 genrule outputs exist.
        urma_so_basenames = [
            "libtpsa", "libummu", "liburma", "liburma_common",
            "liburma_ubagg", "liburma-udma",
        ]
        for base in urma_so_basenames:
            for suffix in [".so", ".so.0"]:
                so_name = base + suffix
                found_path = _find_file_via_shell(repository_ctx, ".", so_name)
                if found_path != "":
                    repository_ctx.symlink(repository_ctx.path(found_path), "lib/" + so_name)
                else:
                    # .so.0 not found; try .so.1 or fall back to .so
                    fb_found = False
                    for fb_suffix in [".so.1", ".so.1.0.5", ".so.0.0.3", ".so.0.0.1", ".so"]:
                        fb_path = _find_file_via_shell(repository_ctx, ".", base + fb_suffix)
                        if fb_path != "":
                            repository_ctx.symlink(repository_ctx.path(fb_path), "lib/" + so_name)
                            fb_found = True
                            break
                    if not fb_found:
                        repository_ctx.file("lib/" + so_name, "")

        build_content = """
package(default_visibility = ["//visibility:public"])

cc_library(
    name = "urma",
    hdrs = glob(["ub/umdk/urma/*.h"]) + glob(["include/ub/umdk/urma/*.h"]),
    includes = ["", "include"],
    srcs = glob([
        "lib/liburma.so*",
        "lib/liburma_ubagg.so*",
        "lib/liburma-udma.so*",
    ]),
)

filegroup(
    name = "urma_libs",
    srcs = glob([
        "lib/libtpsa.so*",
        "lib/libummu.so*",
        "lib/liburma.so*",
        "lib/liburma_common.so*",
        "lib/liburma_ubagg.so*",
        "lib/liburma-udma.so*",
    ]),
)
"""
        repository_ctx.file("BUILD.bazel", build_content)
    else:
        # --- System mode: symlink headers and 6 so files from /usr ---
        repository_ctx.symlink(repository_ctx.path("/usr/include"), "include")

        # Check if URMA is installed. If not, produce an empty repo so
        # non-urma builds work. If --config=urma is used without URMA,
        # the build fails later at compile/link time.
        urma_so_anchor = repository_ctx.path("/usr/lib64/liburma.so")
        if not urma_so_anchor.exists:
            build_content = """
package(default_visibility = ["//visibility:public"])

cc_library(
    name = "urma",
    hdrs = glob(["include/ub/umdk/urma/*.h"]),
    includes = ["include"],
)

filegroup(
    name = "urma_libs",
    srcs = [],
)
"""
            repository_ctx.file("BUILD.bazel", build_content)
            return

        # Symlink the 6 urma so files and their .so/.so.0 versions from
        # /usr/lib64 into lib/. Ensure all 12 declared genrule outputs exist:
        # if .so.0 is missing, try .so.1 or fall back to the .so itself.
        urma_so_names = [
            "libtpsa", "libummu", "liburma", "liburma_common",
            "liburma_ubagg", "liburma-udma",
        ]
        for base in urma_so_names:
            for suffix in [".so", ".so.0"]:
                so_name = base + suffix
                src_path = repository_ctx.path("/usr/lib64/" + so_name)
                if src_path.exists:
                    repository_ctx.symlink(src_path, "lib/" + so_name)
                else:
                    # .so.0 missing: try .so.1, .so.1.0.5, or fall back to .so
                    found = False
                    for fb in [".so.1", ".so.1.0.5", ".so.0.0.3", ".so.0.0.1", ".so"]:
                        fb_path = repository_ctx.path("/usr/lib64/" + base + fb)
                        if fb_path.exists:
                            repository_ctx.symlink(fb_path, "lib/" + so_name)
                            found = True
                            break
                    if not found:
                        # Last resort: create an empty file so genrule outs exist
                        repository_ctx.file("lib/" + so_name, "")

        build_content = """
package(default_visibility = ["//visibility:public"])

cc_library(
    name = "urma",
    hdrs = glob(["include/ub/umdk/urma/*.h"]),
    includes = ["include"],
    srcs = glob([
        "lib/liburma.so*",
        "lib/liburma_ubagg.so*",
        "lib/liburma-udma.so*",
    ]),
)

filegroup(
    name = "urma_libs",
    srcs = glob([
        "lib/libtpsa.so*",
        "lib/libummu.so*",
        "lib/liburma.so*",
        "lib/liburma_common.so*",
        "lib/liburma_ubagg.so*",
        "lib/liburma-udma.so*",
    ]),
)
"""
        repository_ctx.file("BUILD.bazel", build_content)


urma_pkg_repository = repository_rule(
    implementation = _urma_pkg_repository_impl,
    local = True,
    environ = ["URMA_PKG_URL", "URMA_PKG_SHA256"],
)
