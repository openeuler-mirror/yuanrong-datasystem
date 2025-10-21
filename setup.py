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
import platform
import stat
import sys

from setuptools import find_packages, setup
from setuptools.command.build_py import build_py
from setuptools.command.egg_info import egg_info
from wheel.bdist_wheel import bdist_wheel as _bdist_wheel

root_dir = os.path.dirname(os.path.realpath(__file__))

version_path = os.path.join(root_dir, 'datasystem', 'VERSION')
with open(version_path, 'r') as f:
    version = f.read()

readme_path = os.path.join(root_dir, 'datasystem', 'README.md')
with open(readme_path, 'r') as f:
    readme = f.read()
readme = '\n'.join(readme.split('\n')[1:])

commit_id = os.getenv('COMMIT_ID', 'None').replace("\n", "")

package_datas = {
    '': ['sdk_lib_list', 'datasystem_worker', '*.py',
         'worker_config.json', 'cluster_config.json', '*so', '*so*',
         'helm_chart/**/*', 'helm_chart/*', 'helm_chart/**/**/*',
         'include/*', 'include/**/**/*', 'include/**/*', 'lib/**/**/*', 'lib/*',
         'cpp_template/**/*', 'cpp_template/*', 'cpp_template/**/**/*', 'cpp_template/**/**/**/*']
}

requires = []


def build_depends():
    """generate python file"""
    commit_file = os.path.join('datasystem', '.commit_id')
    with os.fdopen(os.open(commit_file, os.O_CREAT | os.O_WRONLY, 0o600), 'w') as file:
        file.write("__commit_id__ = '{}'\n".format(commit_id))
    os.chmod(commit_file, mode=stat.S_IREAD)


build_depends()


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
            if not os.path.islink(file_fullpath):
                os.chmod(file_fullpath, stat.S_IREAD)


class EggInfo(egg_info):
    """Egg info."""

    def run(self):
        egg_info_dir = os.path.join(
            os.path.dirname(__file__), 'yr_datasystem.egg-info')
        super().run()
        update_permissions(egg_info_dir)


class BuildPy(build_py):
    """Build py files."""

    def run(self):
        datasystem_lib_dir = os.path.join(os.path.dirname(__file__), 'build')
        super().run()
        update_permissions(datasystem_lib_dir)
        worker_bin = os.path.join(
            datasystem_lib_dir, 'lib', 'datasystem', 'datasystem_worker')
        os.chmod(worker_bin, stat.S_IREAD | stat.S_IWRITE | stat.S_IEXEC)
        os.system(f"strip --strip-all {worker_bin}")


class CustomBdistWheel(_bdist_wheel):
    def get_tag(self):
        python_tag = f"cp{sys.version_info.major}{sys.version_info.minor}"
        abi_tag = python_tag
        system = platform.system().lower()
        machine = platform.machine().lower()
        if system == "linux":
            plat_tag = f"manylinux2014_{machine}"
        return python_tag, abi_tag, plat_tag


setup(
    name="yr-datasystem",
    version=version,
    author="yr-datasystem Team",
    description=(
        "yr-datasystem is a distributed heterogeneous cache system that supports pooled caching across "
        "HBM, DDR, and SSD, along with asynchronous, high-efficiency data transmission for NPUs."
    ),
    long_description=readme,
    long_description_content_type="text/markdown",
    url="https://gitee.com/openeuler/yuanrong-datasystem",
    python_requires='>=3.8',
    packages=find_packages(),
    package_data=package_datas,
    include_package_data=True,
    cmdclass={
        'egg_info': EggInfo,
        'build_py': BuildPy,
        'bdist_wheel': CustomBdistWheel
    },
    entry_points={
        'console_scripts': [
            'dscli=datasystem.cli.command:main'
        ]
    },
    install_requires=requires,
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Science/Research',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3.8',
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
    keywords='yr-datasystem datasystem yuanrong-datasystem',
)
