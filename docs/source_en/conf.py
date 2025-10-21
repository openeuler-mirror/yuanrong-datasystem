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

import os
import shutil
import sys

import datasystem

# Print debug information
print(f"Python path: {sys.path}")
print(f"datasystem exists: {datasystem.__path__}")

# ---- common
yr_datasystem_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
yr_datasystem_build_dir = os.path.join(os.path.dirname(__file__), "build")
if os.path.exists(yr_datasystem_build_dir):
    if os.path.isdir(yr_datasystem_build_dir):
        shutil.rmtree(yr_datasystem_build_dir)
    else:
        os.remove(yr_datasystem_build_dir)

# ---- process header files
yr_datasystem_include_dir = os.path.join(yr_datasystem_root, "include")
tmp_include_dir = os.path.join(yr_datasystem_build_dir, "include")

shutil.copytree(yr_datasystem_include_dir, tmp_include_dir)

fileList = []
for root, _, files in os.walk(tmp_include_dir):
    for fileObj in files:
        fileList.append(os.path.join(root, fileObj))

for file_name in fileList:
    with open(file_name, 'r') as file:
        content = file.read()  # Read the file content

    # Replace the target string with an empty string
    updated_content = content.replace('__attribute((visibility("default"))) ', '')

    # Open the file for writing and write the modified content back
    with open(file_name, 'w') as file:
        file.write(updated_content)

# ---- Project Information ----
project = 'yr-datasystem'
copyright = 'yr-datasystem'
author = 'yr-datasystem'
release = '1.0'

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
myst_enable_extensions = ["dollarmath", "amsmath", "colon_fence"]

# ---- Extension Configuration ----
extensions = [
    'breathe',
    'exhale',
    'sphinx.ext.autodoc',       # Core autodoc generation
    'sphinx.ext.autosummary',   # Auto summary generation
    'sphinx.ext.viewcode',      # Add source code links
    'sphinx.ext.napoleon',      # Support Google-style docs
    'myst_parser',
    'sphinx_design'
]

# Autosummary configuration
autosummary_generate = True          # Auto-generate summary files
autosummary_imported_members = True  # Include imported members
autosummary_ignore_module_all = False

# Autodoc configuration
autodoc_default_options = {
    'members': True,
    'undoc-members': True,
    'show-inheritance': True
}
autoclass_content = 'both'  # Include both class and __init__ docs

# ---- Template Configuration ----
templates_path = [os.path.realpath('_templates')]

# ---- File Configuration ----
source_suffix = {
    '.rst': 'restructuredtext',
    '.md': 'markdown',
}
master_doc = 'index'
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']

# Setup the breathe extension
breathe_projects = {
    "MyProject": os.path.join(yr_datasystem_build_dir, "doxyoutput", "xml")
}

breathe_default_project = "MyProject"

exhale_args = {
    'containmentFolder': os.path.join(yr_datasystem_build_dir, 'generate'),  # API 文档生成的目录
    'rootFileName': 'index.rst',   # 根文件名
    'rootFileTitle': 'C++ API Documentation',  # 根文件标题
    'doxygenStripFromPath': '../',  # 去除 Doxygen 路径
    'exhaleExecutesDoxygen': True,  # 自动执行 Doxygen
    'exhaleDoxygenStdin': f'''
    INPUT = {tmp_include_dir}
    PROJECT_NAME = MyProject
    GENERATE_XML = YES
    EXTRACT_ALL = NO
    '''
}

# ---- HTML Output Configuration ----
html_theme = 'sphinx_rtd_theme'
html_static_path = ['_static']

# ---- Debug Functions ----
def setup(app):
    # Print all processed objects
    def log_autodoc(app, what, name, obj, options, lines):
        print(f"[AUTODOC] Processing {what}: {name}")
    app.connect('autodoc-process-docstring', log_autodoc)

    # Verify DsClient can be imported
    try:
        from datasystem import DsClient
        print(f"✅ Successfully imported DsClient from {DsClient.__module__}")
    except ImportError as e:
        print(f"❌ Failed to import DsClient: {e}")

# ---- Advanced Configuration ----
# Use mock for dependency resolution if needed
autodoc_mock_imports = []

# Preserve original formatting in docstrings
autodoc_preserve_defaults = True