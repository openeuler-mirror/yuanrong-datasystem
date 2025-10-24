#!/usr/bin/env python
# coding=utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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
import stat
from setuptools import find_packages
from setuptools import setup
from setuptools.command.egg_info import egg_info
from setuptools.command.build_py import build_py

pwd = os.path.dirname(os.path.realpath(__file__))

version_path = os.path.join(pwd, 'datasystem', 'VERSION')
with open(version_path, 'r') as v:
    version = v.read()

package_name = 'yr-datasystem-sdk'
commit_id = os.getenv('COMMIT_ID', 'None').replace("\n", "")

package_datas = {
    '': [
        '*.so*',
        'lib/*.so*',
        '.commit_id',
    ]
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
    Update permissions.

    Args:
        path (str): Target directory path.
    """
    if not os.path.isdir(path):
        raise RuntimeError(r"path: {} is not a directory.".format(path))

    for dirpath, dirnames, filenames in os.walk(path):
        for dirname in dirnames:
            dir_fullpath = os.path.join(dirpath, dirname)
            if not os.path.islink(dir_fullpath):
                os.chmod(dir_fullpath, stat.S_IREAD | stat.S_IWRITE | stat.S_IEXEC)
        for filename in filenames:
            file_fullpath = os.path.join(dirpath, filename)
            if not os.path.islink(file_fullpath):
                os.chmod(file_fullpath, stat.S_IREAD)


class EggInfo(egg_info):
    """Egg info."""

    def run(self):
        egg_info_dir = os.path.join(os.path.dirname(__file__), 'yr_datasystem_sdk.egg-info')
        super().run()
        update_permissions(egg_info_dir)


class BuildPy(build_py):
    """Build py files."""

    def run(self):
        datasystem_lib_dir = os.path.join(os.path.dirname(__file__), 'build', 'lib', 'datasystem')
        super().run()
        update_permissions(datasystem_lib_dir)


setup(
    python_requires='>=3.6',
    name=package_name,
    version=version,
    packages=find_packages(),
    package_data=package_datas,
    include_package_data=True,
    cmdclass={
        'egg_info': EggInfo,
        'build_py': BuildPy,
    },
    install_requires=requires,
)
