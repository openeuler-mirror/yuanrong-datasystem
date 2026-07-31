#!/usr/bin/env python3
# pylint: disable=print-statement

"""Mermaid syntax scanner.

Checks violations per the overview template's "Mermaid syntax constraints":
sequenceDiagram message text must not contain commas/parentheses/semicolons,
and each par branch must contain a complete request + response. Commas in
flowchart edge labels and Note over are whitelisted as safe.

Usage:
  python mermaid_lint.py <doc.md> [--fix-hints]
"""

import argparse
import re
import sys


def extract_mermaid_blocks(text: str):
    """Extract all mermaid code blocks; returns [(start_line, content), ...]."""
    blocks = []
    lines = text.split('\n')
    i = 0
    while i < len(lines):
        if lines[i].strip().startswith('```mermaid'):
            start = i + 1
            buf = []
            i += 1
            while i < len(lines) and not lines[i].strip().startswith('```'):
                buf.append(lines[i])
                i += 1
            blocks.append((start, '\n'.join(buf)))
        i += 1
    return blocks


def lint_block(content: str, base_line: int) -> list:
    """Check a single mermaid block; returns [(line_no, msg, fix_hint), ...]."""
    findings = []
    lines = content.split('\n')
    is_sequence = any('sequenceDiagram' in line for line in lines)
    in_par = False
    par_branch_has_response = False

    for idx, line in enumerate(lines):
        ln = base_line + idx
        stripped = line.strip()

        # par block tracking
        if stripped.startswith('par'):
            in_par = True
            par_branch_has_response = False
        elif stripped.startswith('and') and in_par:
            if not par_branch_has_response:
                findings.append((ln, 'par branch missing response: previous and branch has no -->> response', None))
            par_branch_has_response = False
        elif stripped == 'end' and in_par:
            if not par_branch_has_response:
                findings.append((ln, 'par last branch missing response', None))
            in_par = False

        # Only check sequenceDiagram message text for commas/parentheses/semicolons
        if is_sequence:
            # Message line: A->>B: text / A-->>B: text / A-xB: text / A--)B: text
            m = re.match(r'^\s*\S+\s*(?:--?>>|--?x|--?\))\s*([^:]+):(.+)$', line)
            if m:
                msg_text = m.group(2)
                if ',' in msg_text:
                    fix = msg_text.replace(',', ' ')
                    findings.append((
                        ln,
                        f'sequenceDiagram message contains comma: {line.strip()}',
                        f'replace with space: ...{fix}'
                    ))
                if '(' in msg_text or ')' in msg_text:
                    findings.append((
                        ln,
                        f'sequenceDiagram message contains parenthesis: {line.strip()}',
                        'remove parenthesis'
                    ))
                if ';' in msg_text:
                    findings.append((
                        ln,
                        f'sequenceDiagram message contains semicolon: {line.strip()}',
                        'remove semicolon'
                    ))

        # par branch response tracking
        if in_par and '-->>' in line:
            par_branch_has_response = True

    return findings


def main():
    ap = argparse.ArgumentParser(description='Mermaid syntax scanner')
    ap.add_argument('doc', help='path to the document')
    ap.add_argument('--fix-hints', action='store_true', help='output fix suggestions')
    args = ap.parse_args()

    with open(args.doc, encoding='utf-8') as f:
        text = f.read()

    blocks = extract_mermaid_blocks(text)
    total_findings = []
    for start_line, content in blocks:
        total_findings.extend(lint_block(content, start_line))

    if not total_findings:
        print(f'[PASS] {len(blocks)} mermaid block(s), 0 violation(s)')
        sys.exit(0)

    for ln, msg, hint in total_findings:
        print(f'[L{ln}] {msg}')
        if args.fix_hints and hint:
            print(f'       hint: {hint}')
    print(f'\n[FAIL] {len(total_findings)} violation(s)')
    sys.exit(1)


if __name__ == '__main__':
    main()
