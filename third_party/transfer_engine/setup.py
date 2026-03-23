#!/usr/bin/env python3
import os
import pathlib
import shutil
import subprocess
import sys
from typing import Iterable

from setuptools import Extension, find_packages, setup
from setuptools.command.build_ext import build_ext
from wheel.bdist_wheel import bdist_wheel as _bdist_wheel

try:
    from packaging import tags
except ImportError:  # pragma: no cover
    from wheel.vendored.packaging import tags

ROOT_DIR = pathlib.Path(__file__).resolve().parent

RUNTIME_DEPS = []
BUILD_DEPS = ["cmake>=3.14", "pybind11>=2.13.6"]


class CMakeBuild(build_ext):
    @staticmethod
    def _copy_runtime_libs(build_temp: pathlib.Path, ext_dir: pathlib.Path) -> None:
        for stale_pattern in ("libabsl*.so*", "libprotobuf.so*", "libp2ptransfer.so*"):
            for stale in ext_dir.glob(stale_pattern):
                if stale.is_file():
                    stale.unlink()

        patterns = [
            "lib/libp2ptransfer.so",
        ]

        copied = set()

        def iter_matches(glob_pattern: str) -> Iterable[pathlib.Path]:
            if "**" in glob_pattern:
                yield from build_temp.glob(glob_pattern)
                return
            yield from build_temp.glob(glob_pattern)

        for pattern in patterns:
            for lib in iter_matches(pattern):
                if not lib.is_file():
                    continue
                target = ext_dir / lib.name
                if target in copied:
                    continue
                shutil.copy2(lib, target)
                copied.add(target)

        if copied:
            print(f"[setup.py] Copied {len(copied)} runtime shared libs into {ext_dir}")

    def build_extension(self, ext: Extension) -> None:
        ext_fullpath = pathlib.Path(self.get_ext_fullpath(ext.name)).resolve()
        ext_dir = ext_fullpath.parent

        build_temp = pathlib.Path(self.build_temp) / ext.name
        build_temp.mkdir(parents=True, exist_ok=True)

        cfg = "Debug" if self.debug else "Release"
        cmake_args = [
            f"-DCMAKE_BUILD_TYPE={cfg}",
            "-DCMAKE_SHARED_LINKER_FLAGS=-s",
            "-DCMAKE_MODULE_LINKER_FLAGS=-s",
            "-DCMAKE_EXE_LINKER_FLAGS=-s",
            "-DTRANSFER_ENGINE_BUILD_PYTHON=ON",
            "-DTRANSFER_ENGINE_BUILD_TESTS=OFF",
            "-DTRANSFER_ENGINE_ENABLE_P2P_THIRD_PARTY=ON",
            f"-DTRANSFER_ENGINE_PYTHON_OUTPUT_DIR={ext_dir}",
            f"-DPython3_EXECUTABLE={sys.executable}",
        ]
        ccache_path = shutil.which("ccache")
        if ccache_path:
            cmake_args.extend([
                f"-DCMAKE_C_COMPILER_LAUNCHER={ccache_path}",
                f"-DCMAKE_CXX_COMPILER_LAUNCHER={ccache_path}",
            ])

        parallel_level = os.environ.get("CMAKE_BUILD_PARALLEL_LEVEL")
        if not parallel_level:
            parallel_level = str(os.cpu_count() or 4)
        build_args = ["--config", cfg, "--target", "_transfer_engine", "--parallel", parallel_level]

        print("[setup.py] Build dependencies:", ", ".join(BUILD_DEPS))
        print("[setup.py] Runtime dependencies:", ", ".join(RUNTIME_DEPS) if RUNTIME_DEPS else "(none)")
        print("[setup.py] P2P backend: ON")
        print("[setup.py] ccache:", ccache_path if ccache_path else "not found")
        print("[setup.py] parallel jobs:", parallel_level)

        subprocess.check_call(["cmake", "-S", str(ROOT_DIR), "-B", str(build_temp), *cmake_args])
        subprocess.check_call(["cmake", "--build", str(build_temp), *build_args])

        self._copy_runtime_libs(build_temp, ext_dir)
        if self.inplace:
            src_pkg_dir = ROOT_DIR / "python" / "yr" / "datasystem"
            self._copy_runtime_libs(build_temp, src_pkg_dir)


class CustomBdistWheel(_bdist_wheel):
    def get_tag(self):
        tag = next(tags.sys_tags())
        return tag.interpreter, tag.abi, tag.platform


setup(
    name="openyuanrong-transfer-engine",
    version="0.1.1",
    description="Python bindings for openyuanrong transfer engine",
    packages=find_packages(where="python", include=["yr", "yr.*"]),
    package_dir={"": "python"},
    include_package_data=True,
    package_data={"yr.datasystem": ["*.so", "*.so.*"]},
    ext_modules=[Extension("yr.datasystem._transfer_engine", sources=[])],
    cmdclass={
        "build_ext": CMakeBuild,
        "bdist_wheel": CustomBdistWheel,
    },
    zip_safe=False,
    install_requires=RUNTIME_DEPS,
    python_requires=">=3.9",
)
