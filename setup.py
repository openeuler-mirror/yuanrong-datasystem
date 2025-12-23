#!/usr/bin/env python
# coding=utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""setup_package."""
import os
import shutil
import stat
import subprocess

from pathlib import Path
from setuptools import find_packages, setup
from setuptools.command.build_py import build_py
from setuptools.command.egg_info import egg_info
from wheel.bdist_wheel import bdist_wheel as _bdist_wheel
from wheel.vendored.packaging import tags

root_dir = os.path.dirname(os.path.realpath(__file__))

version_path = os.path.join(root_dir, 'yr', 'datasystem', 'VERSION')
with open(version_path, 'r') as f:
    version = f.read()

readme_path = os.path.join(root_dir, 'yr', 'datasystem', 'README.md')
with open(readme_path, 'r') as f:
    readme = f.read()
readme = '\n'.join(readme.split('\n')[1:])

commit_id = os.getenv('COMMIT_ID', 'None').replace("\n", "")


def recursive_package_files(directory):
    """recursive package files"""
    paths = []
    lib_root_dir = os.path.dirname(os.path.abspath(__file__))
    lib_root_dir = os.path.join(lib_root_dir, 'yr', 'datasystem')
    full_dir = os.path.join(lib_root_dir, directory)
    for root, _, files in os.walk(full_dir):
        for file_name in files:
            rel_path = os.path.relpath(os.path.join(root, file_name), lib_root_dir)
            paths.append(rel_path)
    return paths


package_datas = {
    '': (
        ['sdk_lib_list', 'datasystem_worker', '*.py',
        'worker_config.json', 'cluster_config.json'] +
        recursive_package_files('include') +
        recursive_package_files('helm_chart') +
        recursive_package_files('lib') +
        recursive_package_files('cpp_template')
    )
}


requires = []


def build_depends():
    """Generate python file"""
    commit_file = os.path.join('yr', 'datasystem', '.commit_id')
    os.makedirs(os.path.dirname(commit_file), exist_ok=True)
    with os.fdopen(os.open(commit_file, os.O_CREAT | os.O_WRONLY, 0o600), 'w') as file:
        file.write("__commit_id__ = '{}'\n".format(commit_id))
    os.chmod(commit_file, mode=stat.S_IREAD)
build_depends()


def _process_single_file(ldd_path, file_path):
    """Process a single file, extract dependencies"""
    dependencies = set()
    result = subprocess.run([ldd_path, str(file_path)],
                          capture_output=True, text=True, check=True)
    output = result.stdout
    for line in output.splitlines():
        line = line.strip()
        if '=>' in line:
            lib_name, lib_path = line.split('=>', 1)
            lib_name = lib_name.strip()
            lib_path = lib_path.strip().split()[0]
            dependencies.add(lib_name)
        else:
            parts = line.split()
            if parts:
                lib_name = parts[0]
                dependencies.add(lib_name)
    return dependencies


def get_dependencies(file_path):
    """
    get dependencieds of file
    """
    dependencies = set()
    ldd_path = shutil.which("ldd")
    if ldd_path is None:
        raise FileNotFoundError("cant find ldd, get dependencies failed")
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"File or directory does not exist: {file_path}")
    if path.is_dir():
        for so_file in path.rglob('*.so'):
            try:
                deps = _process_single_file(ldd_path, so_file)
                dependencies.update(deps)
            except subprocess.CalledProcessError:
                continue
            except Exception as e:
                raise RuntimeError(f"Error processing {file_path}: {e}") from e
    elif path.is_file():
        dependencies.update(_process_single_file(ldd_path, path))
    else:
        raise FileNotFoundError(f"Path is not a regular file or directory: {file_path}")
    return dependencies


def get_all_dependencies():
    """
    get all dependencies for datasystem
    """
    all_dependencies = {"libdatasystem.so", "libds_client_py.so", "libacl_plugin.so"}
    src = os.path.join(os.path.dirname(__file__), 'yr', 'datasystem', 'lib')
    worker = os.path.join(os.path.dirname(__file__), 'yr', 'datasystem', 'datasystem_worker')
    src_path = Path(src)
    bin_path = Path(worker)
    all_dependencies.update(get_dependencies(bin_path))
    for item in src_path.rglob('*'):
        all_dependencies.update(get_dependencies(item))
    return all_dependencies
all_dependencies_for_datasystem = get_all_dependencies()


def delete_unuse_so(directory: Path):
    """delete unuse so"""
    for item in directory.iterdir():
        if item.is_file():
            # skip ucx so
            if item.name.startswith("libuc"):
                continue
            if item.name not in all_dependencies_for_datasystem:
                item.unlink()
        elif item.is_dir():
            delete_unuse_so(item)


def check_and_refactor_ucx_lib(lib_path):
    """
    check and refactor ucx lib
    """
    ucp_file_path = os.path.join(lib_path, "libucp.so")
    if not os.path.exists(ucp_file_path):
        return

    ucx_lib_path = os.path.join(lib_path, "ucx")
    os.makedirs(ucx_lib_path, exist_ok=True)

    src_path = Path(lib_path)
    ucp_so_files = list(src_path.glob("libuct_*")) + list(src_path.glob("libucx_*"))
    for file_path in ucp_so_files:
        shutil.move(str(file_path), os.path.join(ucx_lib_path, file_path.name))


def update_permissions(path):
    """
    Update the permissions of files and directories within the specified path.

    Parameters:
        path (str): The target directory path for which permissions will be updated.
    """
    if not os.path.isdir(path):
        raise RuntimeError(r"path: {} is not a directory.".format(path))

    for dirpath, dirnames, filenames in os.walk(path):
        for dirname in dirnames:
            dir_fullpath = os.path.join(dirpath, dirname)
            if not os.path.islink(dir_fullpath):
                os.chmod(dir_fullpath, stat.S_IREAD |
                         stat.S_IWRITE | stat.S_IEXEC)
        for filename in filenames:
            file_fullpath = os.path.join(dirpath, filename)
            if os.path.islink(file_fullpath):
                continue
            os.chmod(file_fullpath, stat.S_IREAD)


class EggInfo(egg_info):
    """Egg info."""

    def run(self):
        egg_info_dir = os.path.join(
            os.path.dirname(__file__), 'openyuanrong_datasystem.egg-info')
        super().run()
        update_permissions(egg_info_dir)


class BuildPy(build_py):
    """Build py files."""

    def run(self):
        datasystem_lib_dir = os.path.join(os.path.dirname(__file__), 'build')
        super().run()
        update_permissions(datasystem_lib_dir)
        worker_bin = os.path.join(
            datasystem_lib_dir, 'lib', 'yr', 'datasystem', 'datasystem_worker')
        os.chmod(worker_bin, stat.S_IREAD | stat.S_IWRITE | stat.S_IEXEC)
        os.system(f"strip --strip-all {worker_bin}")
        lib_dir = os.path.join(os.path.dirname(__file__), 'build', 'lib', 'yr', 'datasystem', 'lib')
        lib_path = Path(lib_dir)
        delete_unuse_so(lib_path)
        check_and_refactor_ucx_lib(lib_dir)


class CustomBdistWheel(_bdist_wheel):
    def get_tag(self):
        tag = next(tags.sys_tags())
        return tag.interpreter, tag.abi, tag.platform


setup(
    name="openyuanrong-datasystem",
    version=version,
    author="openYuanrong Team",
    description=(
        "openYuanrong datasystem is a distributed heterogeneous cache system that supports pooled caching across "
        "HBM, DDR, and SSD, along with asynchronous, high-efficiency data transmission for NPUs."
    ),
    long_description=readme,
    long_description_content_type="text/markdown",
    url="https://gitee.com/openeuler/yuanrong-datasystem",
    python_requires='>=3.9',
    packages=find_packages(include=['yr*', 'yr.*']),
    package_data=package_datas,
    include_package_data=True,
    cmdclass={
        'egg_info': EggInfo,
        'build_py': BuildPy,
        'bdist_wheel': CustomBdistWheel
    },
    entry_points={
        'console_scripts': [
            'dscli=yr.datasystem.cli.command:main'
        ]
    },
    install_requires=requires,
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Science/Research',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: C++',
        'Topic :: Scientific/Engineering',
        'Topic :: Software Development',
        'Topic :: Software Development :: Libraries',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
    license="Apache 2.0",
    keywords='openyuanrong-datasystem datasystem',
)
