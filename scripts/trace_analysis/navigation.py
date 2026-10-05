"""Connect a completed report bundle using verified relative links."""
import html
import os
from pathlib import Path
import re
from urllib.parse import quote

from .resources import asset_path

LABELS = {'triage': 'Trace Triage', 'read': '读取分析', 'write': '写入分析', 'numa': 'NUMA 分析'}


def link_reports(reports):
    paths = {key: Path(reports[key]).resolve() for key in LABELS}
    style = asset_path('report_switcher.css').read_text(encoding='utf-8')
    prepared = {}
    for key, path in paths.items():
        text = path.read_text(encoding='utf-8')
        text = re.sub(r'<!-- report-switcher:start -->.*?<!-- report-switcher:end -->', '', text, flags=re.S)
        links = []
        for target, label in LABELS.items():
            href = quote(os.path.relpath(paths[target], path.parent).replace(os.sep, '/'), safe='/')
            current = ' aria-current="page"' if target == key else ''
            links.append(f'<a href="{html.escape(href, quote=True)}"{current}>{label}</a>')
        bar = (
            '<!-- report-switcher:start --><style>'
            + style
            + '</style><div id="report-switcher" role="navigation" aria-label="分析页面切换">'
            + ''.join(links)
            + '</div><!-- report-switcher:end -->'
        )
        # Insert before the real body content, never into the embedded chart library.
        text, count = re.subn(r'(<body\b[^>]*>)', lambda m: m[0] + bar, text, count=1, flags=re.I)
        if not count:
            raise ValueError(f'missing body element: {path}')
        prepared[path] = text
    for path, text in prepared.items():
        path.write_text(text, encoding='utf-8')
