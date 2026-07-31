#!/usr/bin/env python3
# pylint: disable=print-statement

"""Overview design out-of-scope check.

Prevents overview design documents from describing detailed-design internals:
class inheritance hierarchies, data-structure field listings, lock-implementation
details, and algorithm-step pseudocode. False positives can be exempted with <!-- scope:allow -->.

Intended only for overview design documents (detailed design is expected to cover internals).

Usage:
  python scope_check.py <doc.md>
"""

import argparse
import re
import sys


# Out-of-scope signal patterns
SCOPE_PATTERNS = [
    # Class inheritance hierarchy: class X : public Y / class X : private Y
    (re.compile(r'\bclass\s+\w+\s*:\s*(public|private|protected)\s+\w+'),
     'inheritance hierarchy', 'Overview design describes only external interfaces, not internal class inheritance'),
    # Lock-implementation details: std::mutex / .Lock() / .Unlock() / lock_guard in body (not interface-signature line)
    (re.compile(r'\b(std::mutex|std::lock_guard|std::unique_lock)\b'),
     'lock-implementation detail',
     'Lock implementation belongs to detailed design; '
     'overview describes only concurrency constraints'),
    (re.compile(r'\.\s*([Ll]ock|[Uu]nlock)\s*\(\s*\)'),
     'lock-implementation detail', '.lock()/.unlock() calls belong to detailed design'),
]

# Data-structure field listing: consecutive "type field;" lines (3+ lines counts as listing)
_FIELD_TYPES = (r'int|uint\d+_t|bool|char|double|float|size_t'
                r'|std::string|std::vector|std::map|void\*')
FIELD_LINE_RE = re.compile(r'^\s*(?:' + _FIELD_TYPES + r')\s*<[^;]*>\s+\w+\s*[;=]'
                            r'|^\s*(?:' + _FIELD_TYPES + r')\s+\w+\s*[;=]')

# Algorithm-step pseudocode: for/if control flow inside a cpp code block
CPP_BLOCK_RE = re.compile(r'```cpp\n(.*?)```', re.DOTALL)
CONTROL_FLOW_RE = re.compile(r'^\s*(for|while|if|else|switch)\s*\(', re.MULTILINE)

ALLOW_MARKER = '<!-- scope:allow -->'


def check_scope(text: str) -> list:
    """Return [(line_no, signal, matched_text, advice), ...]."""
    findings = []
    lines = text.split('\n')

    # First find exemption ranges: lines containing ALLOW_MARKER, plus 3 lines above/below, are exempt
    exempt_lines = set()
    for idx, line in enumerate(lines):
        if ALLOW_MARKER in line:
            for j in range(max(0, idx - 3), min(len(lines), idx + 4)):
                exempt_lines.add(j)

    # 1. Regex pattern check
    for idx, line in enumerate(lines):
        if idx in exempt_lines:
            continue
        for pat, signal, advice in SCOPE_PATTERNS:
            m = pat.search(line)
            if m:
                findings.append((idx + 1, signal, line.strip(), advice))

    # 2. Data-structure field listing (3+ consecutive lines)
    consecutive = 0
    block_start = 0
    for idx, line in enumerate(lines):
        if idx in exempt_lines:
            consecutive = 0
            continue
        if FIELD_LINE_RE.match(line):
            if consecutive == 0:
                block_start = idx
            consecutive += 1
        else:
            if consecutive >= 3:
                findings.append((
                    block_start + 1,
                    'data-structure field listing',
                    f'{consecutive} consecutive field line(s)',
                    'data-structure definition belongs to detailed design'
                ))
            consecutive = 0
    if consecutive >= 3:
        findings.append((
            block_start + 1,
            'data-structure field listing',
            f'{consecutive} consecutive field line(s)',
            'data-structure definition belongs to detailed design'
        ))

    # 3. Algorithm pseudocode inside a cpp code block (for/if control flow)
    for m in CPP_BLOCK_RE.finditer(text):
        block = m.group(1)
        block_start_line = text[:m.start()].count('\n') + 2
        if CONTROL_FLOW_RE.search(block):
            # Find the lines containing control flow
            for cf_m in CONTROL_FLOW_RE.finditer(block):
                cf_line_in_block = block[:cf_m.start()].count('\n')
                abs_line = block_start_line + cf_line_in_block
                if abs_line - 1 not in exempt_lines:
                    findings.append((abs_line, 'algorithm-step pseudocode',
                                     cf_m.group(0).strip(), 'algorithm control flow belongs to detailed design'))

    return findings


def main():
    ap = argparse.ArgumentParser(description='Overview design out-of-scope check')
    ap.add_argument('doc', help='path to the overview design document')
    args = ap.parse_args()

    with open(args.doc, encoding='utf-8') as f:
        text = f.read()

    findings = check_scope(text)
    if not findings:
        print('[PASS] 0 out-of-scope item(s)')
        sys.exit(0)

    for ln, signal, matched, advice in findings:
        print(f'[L{ln}] out-of-scope ({signal}): {matched}')
        print(f'       {advice}. Add {ALLOW_MARKER} to exempt a false positive')
    print(f'\n[FAIL] {len(findings)} out-of-scope item(s)')
    sys.exit(1)


if __name__ == '__main__':
    main()
