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

# The 6 URMA shared libraries shipped in a URMA SDK package. The real SONAME
# of each may be any .so.N (e.g. libummu.so.1 while the others are .so.0), and
# transitive DT_NEEDED references the exact SONAME — so we preserve every
# shipped variant rather than hardcoding .so/.so.0/.so.1.
_URMA_SO_BASES = [
    "libtpsa", "libummu", "liburma", "liburma_common",
    "liburma_ubagg", "liburma-udma",
]

def _symlink_urma_libs(repository_ctx, lib_dir):
    """Symlink every shipped lib<base>.so* variant from lib_dir into lib/.

    lib_dir is a flat directory (the package's lib root in download mode, or
    /usr/lib64 in system mode). Uses `find -maxdepth 1 -name lib<base>.so*`
    (no recursion, no -L) so only the 6 URMA libs' own variants are picked
    up — unrelated system libs are never引入. Each found file/symlink is
    symlinked into lib/ under its own basename, preserving the original
    SONAME so link/load-time DT_NEEDED always resolves. Missing variants are
    simply skipped (no empty-file fallback — packaging no longer relies on a
    fixed set of declared outputs).
    """
    for base in _URMA_SO_BASES:
        pattern = base + ".so*"
        # -maxdepth 1: stay in lib_dir (no recursion into subdirs / no chasing
        #              symlinks elsewhere). -type f OR -type l: catch both the
        #              real versioned files and the dev symlink lib<base>.so.
        find_result = repository_ctx.execute(
            ["find", lib_dir, "-maxdepth", "1", "-name", pattern,
             "(", "-type", "f", "-o", "-type", "l", ")"],
            quiet = True,
        )
        if find_result.return_code != 0:
            continue
        for line in find_result.stdout.strip().split("\n"):
            line = line.strip()
            if not line:
                continue
            basename = line.rsplit("/", 1)[-1]
            target = "yr/datasystem/lib/" + basename
            if not repository_ctx.path(target).exists:
                repository_ctx.symlink(repository_ctx.path(line), target)

def _urma_pkg_repository_impl(repository_ctx):
    urma_pkg_url = repository_ctx.os.environ.get("URMA_PKG_URL", "")
    urma_pkg_sha256 = repository_ctx.os.environ.get("URMA_PKG_SHA256", "")

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

        # Preserve EVERY shipped lib<base>.so* variant under lib/, so the real
        # SONAME (whatever it is — .so.0 / .so.1 / .so.2 / .so.1.0.5 ...) is
        # always reachable at link/load time. Transitive DT_NEEDED references
        # the exact SONAME; hardcoding .so+.so.0 breaks whenever the package's
        # SONAME differs (e.g. libummu.so.1 here). Globbing lib_dir (maxdepth 1,
        # no recursion, no -L) makes it future-proof against URMA SONAME bumps
        # without pulling in unrelated package files.
        _symlink_urma_libs(repository_ctx, lib_dir)

        build_content = """
package(default_visibility = ["//visibility:public"])

cc_library(
    name = "urma",
    hdrs = glob(["ub/umdk/urma/*.h"]) + glob(["include/ub/umdk/urma/*.h"]),
    includes = ["", "include"],
    srcs = glob([
        "yr/datasystem/lib/liburma.so*",
        "yr/datasystem/lib/liburma_ubagg.so*",
        "yr/datasystem/lib/liburma-udma.so*",
    ]),
)

filegroup(
    name = "urma_libs",
    srcs = glob([
        "yr/datasystem/lib/libtpsa.so*",
        "yr/datasystem/lib/libummu.so*",
        "yr/datasystem/lib/liburma.so*",
        "yr/datasystem/lib/liburma_common.so*",
        "yr/datasystem/lib/liburma_ubagg.so*",
        "yr/datasystem/lib/liburma-udma.so*",
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

        # Symlink EVERY shipped lib<base>.so* variant from /usr/lib64 into lib/.
        # Preserve the real SONAME (whatever it is) so transitive DT_NEEDED
        # (e.g. liburma-udma.so -> libummu.so.1) always resolves. Globbing all
        # .so* (via _symlink_urma_libs) is future-proof against SONAME bumps;
        # hardcoding .so/.so.0/.so.1 breaks when the SONAME is something else.
        _symlink_urma_libs(repository_ctx, "/usr/lib64")

        build_content = """
package(default_visibility = ["//visibility:public"])

cc_library(
    name = "urma",
    hdrs = glob(["include/ub/umdk/urma/*.h"]),
    includes = ["include"],
    srcs = glob([
        "yr/datasystem/lib/liburma.so*",
        "yr/datasystem/lib/liburma_ubagg.so*",
        "yr/datasystem/lib/liburma-udma.so*",
    ]),
)

filegroup(
    name = "urma_libs",
    srcs = glob([
        "yr/datasystem/lib/libtpsa.so*",
        "yr/datasystem/lib/libummu.so*",
        "yr/datasystem/lib/liburma.so*",
        "yr/datasystem/lib/liburma_common.so*",
        "yr/datasystem/lib/liburma_ubagg.so*",
        "yr/datasystem/lib/liburma-udma.so*",
    ]),
)
"""
        repository_ctx.file("BUILD.bazel", build_content)


urma_pkg_repository = repository_rule(
    implementation = _urma_pkg_repository_impl,
    local = True,
    environ = ["URMA_PKG_URL", "URMA_PKG_SHA256"],
)
