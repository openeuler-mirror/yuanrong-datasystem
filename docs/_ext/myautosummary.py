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
"""
Customized autosummary directives for sphinx.
"""

import os
import re
from typing import List, Tuple

from docutils.nodes import Node
from sphinx.ext.autodoc.directive import DocumenterBridge, Options
from sphinx.ext.autosummary import (Autosummary, Matcher, ModuleAnalyzer,
                                    ModuleType, PycodeError, StringList,
                                    addnodes, autosummary_table,
                                    autosummary_toc, extract_summary,
                                    get_documenter,
                                    get_import_prefixes_from_env,
                                    import_by_name, logger, mangle_signature,
                                    mock, nodes, posixpath, rst,
                                    switch_source_input)
from sphinx.locale import __


MAX_ITEM_CHARS = 50


class DsAutosummary(Autosummary):
    """
    Inherited from sphinx's autosummary, add titles and a column for the generated table.
    """

    def init(self):
        """
        init method
        """
        self.find_doc_name = ""
        self.third_title = ""
        self.default_doc = ""

    def extract_env_summary(self, doc: List[str]) -> str:
        """Extract env summary from docstring."""
        env_sum = self.default_doc
        for i, piece in enumerate(doc):
            if piece.startswith(self.find_doc_name):
                env_sum = doc[i+1][4:]
        return env_sum

    def run(self):
        """
        run method
        """
        self.init()
        self.bridge = DocumenterBridge(self.env, self.state.document.reporter,
                                       Options(), self.lineno, self.state)

        names = [x.strip().split()[0] for x in self.content if x.strip() and re.search(r'^[~a-zA-Z_]', x.strip()[0])]
        items = self.get_items(names)
        teble_nodes = self.get_table(items)

        if 'toctree' not in self.options:
            return teble_nodes

        dirname = posixpath.dirname(self.env.docname)

        tree_prefix = self.options['toctree'].strip()
        docnames = []
        excluded = Matcher(self.config.exclude_patterns)
        for item in items:
            docname = posixpath.join(tree_prefix, item[3])
            docname = posixpath.normpath(posixpath.join(dirname, docname))
            if docname not in self.env.found_docs:
                location = self.state_machine.get_source_and_line(self.lineno)
                if excluded(self.env.doc2path(docname, None)):
                    msg = __('autosummary references excluded document %r. Ignored.')
                else:
                    msg = __('autosummary: stub file not found %r. '
                                'Check your autosummary_generate setting.')
                logger.warning(msg, item[3], location=location)
                continue
            docnames.append(docname)

        if docnames:
            tocnode = addnodes.toctree()
            tocnode['includefiles'] = docnames
            tocnode['entries'] = [(None, docn) for docn in docnames]
            tocnode['maxdepth'] = -1
            tocnode['glob'] = None
            teble_nodes.append(autosummary_toc('', '', tocnode))
        return teble_nodes

    def get_items(self, names: List[str]) -> List[Tuple[str, str, str, str, str]]:
        """Try to import the given names, and return a list of
        ``[(name, signature, summary_string, real_name, env_summary), ...]``.
        """
        prefixes = get_import_prefixes_from_env(self.env)
        items = []  # type: List[Tuple[str, str, str, str, str]]

        for name in names:
            display_name = name
            if name.startswith('~'):
                name = name[1:]
                display_name = name.split('.')[-1]
            try:
                with mock(self.config.autosummary_mock_imports):
                    real_name, obj, parent, modname = import_by_name(name, prefixes=prefixes)
            except ImportError:
                logger.warning(__('failed to import %s'), name)
                items.append((name, '', '', name, ''))
                continue

            self.bridge.result = StringList()  # initialize for each documenter
            # give explicitly separated module name, so that members
            # of inner classes can be documented
            full_name = f"{modname}::{full_name[len(modname) + 1:]}" if not isinstance(obj, ModuleType) else real_name
            # NB. using full_name here is important, since Documenters
            #     handle module prefixes slightly differently
            doccls = get_documenter(self.env.app, obj, parent)
            documenter = doccls(self.bridge, full_name)

            if not documenter.parse_name():
                logger.warning(__('failed to parse name %s'), real_name)
                items.append((display_name, '', '', real_name, ''))
                continue
            if not documenter.import_object():
                logger.warning(__('failed to import object %s'), real_name)
                items.append((display_name, '', '', real_name, ''))
                continue
            if documenter.options.members and not documenter.check_module():
                continue

            # try to also get a source code analyzer for attribute docs
            try:
                documenter.analyzer = ModuleAnalyzer.for_module(documenter.get_real_modname())
                # parse right now, to get PycodeErrors on parsing (results will
                # be cached anyway)
                documenter.analyzer.find_attr_docs()
            except PycodeError as err:
                logger.debug('[autodoc] module analyzer failed: %s', err)
                # no source file -- e.g. for builtin and C modules
                documenter.analyzer = None

            # -- Grab the signature

            try:
                sig = documenter.format_signature(show_annotation=False)
            except TypeError:
                # the documenter does not support ``show_annotation`` option
                sig = documenter.format_signature()

            if not sig:
                sig = ''
            else:
                sig = mangle_signature(sig, max_chars=max(10, MAX_ITEM_CHARS - len(display_name)))

            # -- Grab the summary

            documenter.add_content(None)
            summary = extract_summary(self.bridge.result.data[:], self.state.document)
            env_sum = self.extract_env_summary(self.bridge.result.data[:])
            items.append((display_name, sig, summary, real_name, env_sum))

        return items

    def get_table(self, items: List[Tuple[str, str, str, str, str]]) -> List[Node]:
        """Generate a proper list of table nodes for autosummary:: directive.

        *items* is a list produced by :meth:`get_items`.
        """
        table_spec = addnodes.tabular_col_spec()
        table_spec['spec'] = r'\X{1}{2}\X{1}{2}'

        table = autosummary_table('')
        real_table = nodes.table('', classes=['longtable'])
        table.append(real_table)
        group = nodes.tgroup('', cols=3)
        real_table.append(group)
        group.append(nodes.colspec('', colwidth=10))
        group.append(nodes.colspec('', colwidth=70))
        group.append(nodes.colspec('', colwidth=30))
        body = nodes.tbody('')
        group.append(body)

        def append_row(*column_texts: str) -> None:
            row = nodes.row('', color="red")
            source, line = self.state_machine.get_source_and_line()
            for text in column_texts:
                node = nodes.paragraph('')
                vl = StringList()
                vl.append(text, '%s:%d:<autosummary>' % (source, line))
                with switch_source_input(self.state, vl):
                    self.state.nested_parse(vl, 0, node)
                    if node and isinstance(node[0], nodes.paragraph):
                        node = node[0]
                    row.append(nodes.entry('', node))
            body.append(row)

        # add table's title
        append_row("**API Name**", "**Description**", self.third_title)
        for name, sig, summary, real_name, env_sum in items:
            qualifier = 'obj'
            if 'nosignatures' not in self.options:
                col1 = ':%s:`%s <%s>`\\ %s' % (qualifier, name, real_name, rst.escape(sig))
            else:
                col1 = ':%s:`%s <%s>`' % (qualifier, name, real_name)
            col2 = summary
            col3 = env_sum
            append_row(col1, col2, col3)

        return [table_spec, table]


class DsNoteAutoSummary(DsAutosummary):
    """
    Inherited from DsAutosummary. Add a third column about `Note` to the table.
    """

    def init(self):
        """
        init method
        """
        self.find_doc_name = ".. note::"
        self.third_title = "**Note**"
        self.default_doc = "None"

    def extract_env_summary(self, doc: List[str]) -> str:
        """Extract env summary from docstring."""
        env_sum = self.default_doc
        for piece in doc:
            if piece.startswith(self.find_doc_name):
                env_sum = piece[10:]
        return env_sum


class DsPlatformAutoSummary(DsAutosummary):
    """
    Inherited from DsAutosummary. Add a third column about `Supported Platforms` to the table.
    """
    def init(self):
        """
        init method
        """
        self.find_doc_name = "Supported Platforms:"
        self.third_title = "**{}**".format(self.find_doc_name[:-1])
        self.default_doc = "``Ascend`` ``GPU`` ``CPU``"


class DsCnAutoSummary(Autosummary):
    """Overwrite MsPlatformAutosummary for chinese python api."""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.table_head = ()
        self.find_doc_name = ""
        self.third_title = ""
        self.default_doc = ""
        self.third_name_en = ""

    def get_third_column_en(self, doc):
        """Get the third column for en."""
        third_column = self.default_doc
        for i, piece in enumerate(doc):
            if piece.startswith(self.third_name_en):
                try:
                    if "eprecated" in doc[i+1][4:]:
                        third_column = "弃用"
                    else:
                        third_column = doc[i+1][4:]
                except IndexError:
                    third_column = ''
        return third_column

    def get_summary_re(self, display_name: str):
        """Get the summary"""
        return re.compile(rf'\.\. \w+:\w+::\s+{display_name}.*?\n\n\s+(.*?)[。\n]')

    def run(self) -> List[Node]:
        """Run this program"""
        self.bridge = DocumenterBridge(self.env, self.state.document.reporter,
                                       Options(), self.lineno, self.state)

        names = [x.strip().split()[0] for x in self.content if x.strip() and re.search(r'^[~a-zA-Z_]', x.strip()[0])]
        items = self.get_items(names)
        table_nodes = self.get_table(items)

        dirname = posixpath.dirname(self.env.docname)

        tree_prefix = self.options['toctree'].strip()
        docnames = []
        names = [i[0] for i in items]
        for name in names:
            docname = posixpath.join(tree_prefix, name)
            docname = posixpath.normpath(posixpath.join(dirname, docname))
            if docname not in self.env.found_docs:
                continue

            docnames.append(docname)

        if docnames:
            tocnode = addnodes.toctree()
            tocnode['includefiles'] = docnames
            tocnode['entries'] = [(None, docn) for docn in docnames]
            tocnode['maxdepth'] = -1
            tocnode['glob'] = None

            table_nodes.append(autosummary_toc('', '', tocnode))

        return table_nodes

    def get_items(self, names: List[str]) -> List[Tuple[str, str, str, str]]:
        """Try to import the given names, and return a list of
        ``[(name, signature, summary_string, real_name), ...]``.
        """
        doc_path = os.path.dirname(self.state.document.current_source)
        prefixes = get_import_prefixes_from_env(self.env)
        items = []  # type: List[Tuple[str, str, str, str]]

        for name in names:
            display_name = name
            if name.startswith('~'):
                name = name[1:]
                display_name = name.split('.')[-1]

            file_path = os.path.normpath(os.path.join(doc_path, self.options['toctree'], display_name+'.rst'))
            if self.if_need_solve(doc_path, display_name):
                items = self.get_summary_info(file_path, display_name, items)
            else:
                try:
                    with mock(self.config.autosummary_mock_imports):
                        real_name, obj, parent, modname = import_by_name(name, prefixes=prefixes)
                except ImportError:
                    logger.warning(__('failed to import %s'), name)
                    items.append((name, '', ''))
                    continue

                self.bridge.result = StringList()  # initialize for each documenter
                full_name = real_name
                if not isinstance(obj, ModuleType):
                    # give explicitly separated module name, so that members
                    # of inner classes can be documented
                    full_name = modname + '::' + full_name[len(modname) + 1:]
                # NB. using full_name here is important, since Documenters
                #     handle module prefixes slightly differently
                doccls = get_documenter(self.env.app, obj, parent)
                documenter = doccls(self.bridge, full_name)

                if not documenter.parse_name():
                    logger.warning(__('failed to parse name %s'), real_name)
                    items.append((display_name, '', ''))
                    continue
                if not documenter.import_object():
                    logger.warning(__('failed to import object %s'), real_name)
                    items.append((display_name, '', ''))
                    continue
                if documenter.options.members and not documenter.check_module():
                    continue

                # try to also get a source code analyzer for attribute docs
                try:
                    documenter.analyzer = ModuleAnalyzer.for_module(documenter.get_real_modname())
                    # parse right now, to get PycodeErrors on parsing (results will
                    # be cached anyway)
                    documenter.analyzer.find_attr_docs()
                except PycodeError as err:
                    logger.debug('[autodoc] module analyzer failed: %s', err)
                    # no source file -- e.g. for builtin and C modules
                    documenter.analyzer = None

                # -- Grab the summary and third_colum

                documenter.add_content(None)
                summary = extract_summary(self.bridge.result.data[:], self.state.document)
                if self.table_head:
                    third_colum = self.get_third_column_en(self.bridge.result.data[:])
                    items.append((display_name, summary, third_colum))
                else:
                    items.append((display_name, summary))

        return items

    def if_need_solve(self, docpath: str, file: str) -> bool:
        """If need solve"""
        dir_name = self.options['toctree']
        origin_rst_files = self.env.config.rst_files
        all_rst_files = self.env.found_docs

        spec_path = os.path.join('api_python', dir_name, file)
        file_path = os.path.join(docpath, dir_name, file+'.rst')
        generated_files = all_rst_files.difference(origin_rst_files)
        return os.path.exists(file_path) and spec_path not in generated_files

    def get_summary_info(
        self, file_path: str, file: str,
        items: List[Tuple[str, str, str,
                          str]]) -> List[Tuple[str, str, str, str]]:
        """Get the summary info."""
        summary_re_tag = re.compile(rf'\.\. \w+:\w+::\s+{file}.*?\n\s+:.*?:\n\n\s+(.*?)[。\n]')
        summary_re_line = re.compile(rf'\.\. \w+:\w+::\s+{file}(?:.|\n|)+?\n\n\s+(.*?)[。\n]')
        summary_re = self.get_summary_re(file)
        content = ''
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        if content:
            summary_str = summary_re.findall(content)
            summary_str_tag = summary_re_tag.findall(content)
            summary_str_line = summary_re_line.findall(content)
            if summary_str:
                if re.findall("[:：,，。.;；]", summary_str[0][-1]):
                    logger.warning(f"{file}接口的概述格式需调整")
                summary_str = summary_str[0] + '。'
            elif summary_str_tag:
                if re.findall("[:：,，。.;；]", summary_str_tag[0][-1]):
                    logger.warning(f"{file}接口的概述格式需调整")
                summary_str = summary_str_tag[0] + '。'
            elif summary_str_line:
                if re.findall("[:：,，。.;；]", summary_str_line[0][-1]):
                    logger.warning(f"{file}接口的概述格式需调整")
                summary_str = summary_str_line[0] + '。'
            else:
                summary_str = ''
            if not self.table_head:
                items.append((file, summary_str))
            else:
                third_str = self.get_third_column(file, content)
                if third_str:
                    third_str = third_str[0]
                else:
                    third_str = ''

                items.append((file, summary_str, third_str))

        return items

    def get_table(self, items: List[Tuple[str, str, str]]) -> List[Node]:
        """Generate a proper list of table nodes for autosummary:: directive.

        *items* is a list produced by :meth:`get_items`.
        """
        table_spec = addnodes.tabular_col_spec()
        table = autosummary_table('')
        real_table = nodes.table('', classes=['longtable'])
        table.append(real_table)

        if not self.table_head:
            table_spec['spec'] = r'\X{1}{2}\X{1}{2}'
            group = nodes.tgroup('', cols=2)
            real_table.append(group)
            group.append(nodes.colspec('', colwidth=10))
            group.append(nodes.colspec('', colwidth=90))
        else:
            table_spec['spec'] = r'\X{1}{2}\X{1}{2}\X{1}{2}'
            group = nodes.tgroup('', cols=3)
            real_table.append(group)
            group.append(nodes.colspec('', colwidth=10))
            group.append(nodes.colspec('', colwidth=60))
            group.append(nodes.colspec('', colwidth=30))
        body = nodes.tbody('')
        group.append(body)

        def append_row(*column_texts: str) -> None:
            row = nodes.row('')
            source, line = self.state_machine.get_source_and_line()
            for text in column_texts:
                node = nodes.paragraph('')
                vl = StringList()
                vl.append(text, '%s:%d:<autosummary>' % (source, line))
                with switch_source_input(self.state, vl):
                    self.state.nested_parse(vl, 0, node)
                    if node and isinstance(node[0], nodes.paragraph):
                        node = node[0]
                    row.append(nodes.entry('', node))
            body.append(row)
        append_row(*self.table_head)
        if not self.table_head:
            try:
                for name, summary in items:
                    qualifier = 'obj'
                    col1 = ':%s:`%s <%s>`' % (qualifier, name, name)
                    col2 = summary
                    append_row(col1, col2)
            except ValueError:
                logger.warning(items)
        else:
            for name, summary, other in items:
                qualifier = 'obj'
                col1 = ':%s:`%s <%s>`' % (qualifier, name, name)
                col2 = summary
                col3 = other
                append_row(col1, col2, col3)
        return [table_spec, table]
