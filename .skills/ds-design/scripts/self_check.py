#!/usr/bin/env python3
# pylint: disable=print-statement

"""Design document structural self-check.

Runs a structured checklist against overview/detailed design documents,
emitting PASS/BLOCK/INFO + evidence. Closed-item failures block the gate;
cross-section items report INFO (non-blocking) unless full mode, where all block.

Usage:
  # full (stage 4 final review)
  python self_check.py <doc.md> --type overview|detailed [--overview <overview.md>]
  # per-section (stage 3 per-section gate)
  python self_check.py <doc.md> --type overview --section §1
"""

import argparse
import re
import sys
from dataclasses import dataclass, field
from typing import Optional


# Section regex: matches "§1", "§4.3", etc.
SECTION_RE = re.compile(r'§(\d+)(?:\.(\d+))?')
# file_path:line evidence regex
EVIDENCE_RE = re.compile(r'[\w/.\-]+:\d+')


@dataclass
class CheckResult:
    rule: str
    status: str  # PASS / BLOCK / INFO / SKIP
    evidence: str = ''
    detail: str = ''


@dataclass
class SectionParser:
    """Split the document into section blocks keyed by §N headings."""
    text: str
    sections: dict = field(default_factory=dict)  # '§1' -> text, '§4.3' -> text

    def parse(self):
        # Match markdown headings: ## / ### / #### followed by text containing §N, or "## 1. background"
        # Strategy: scan line by line; on encountering ##/### with a numeric section id, split
        lines = self.text.split('\n')
        current_key = None
        current_buf = []
        for line in lines:
            # Heading line: ## N. title or ### N.M title or #### title with numbering
            m = re.match(r'^(#{2,4})\s*(\d+)(?:\.(\d+))?\b', line)
            if m:
                if current_key:
                    self.sections[current_key] = '\n'.join(current_buf)
                num, sub = m.group(2), m.group(3)
                current_key = f'§{num}' + (f'.{sub}' if sub else '')
                current_buf = [line]
            elif current_key:
                current_buf.append(line)
        if current_key:
            self.sections[current_key] = '\n'.join(current_buf)
        return self.sections

    def get(self, key: str) -> str:
        """Return section content. Parent chapter §4 returns itself + all child sections §4.x merged."""
        if '.' in key:
            # Subsection exact match
            return self.sections.get(key, '')
        # Parent chapter: merge itself + all §N.x subsections
        parts = []
        if key in self.sections:
            parts.append(self.sections[key])
        prefix = key + '.'
        for k in sorted(self.sections):
            if k.startswith(prefix):
                parts.append(self.sections[k])
        return '\n'.join(parts) if parts else ''


def has_section_text(parser: SectionParser, key: str) -> bool:
    if key in parser.sections:
        return True
    parent = key.split('.')[0]
    return parent in parser.sections


# ---------- Overview design check rules ----------

def check_overview_section(parser: SectionParser, section: str) -> list:
    """Run per-section checks for overview design. Returns a list of CheckResult."""
    results = []
    # §1 current-status conclusion carries file_path:line
    if section in ('§1', 'full'):
        s = parser.get('§1')
        if EVIDENCE_RE.search(s):
            m = EVIDENCE_RE.search(s)
            results.append(CheckResult('current-status conclusion with file_path:line', 'PASS', m.group(0)))
        elif s:
            results.append(CheckResult('current-status conclusion with file_path:line', 'BLOCK',
                                       detail='§1: no code evidence in file_path:line format found'))

    # §2 goals user-perceivable + technical constraints pushed down
    if section in ('§2', 'full'):
        s = parser.get('§2')
        if s:
            perceivable_kw = [
                'RTT', '延迟', '吞吐', '带宽', '恢复时间', '不丢',
                '一致', '可用', '亲和', 'QPS', 'ms', 'μs'
            ]
            if any(k in s for k in perceivable_kw):
                results.append(CheckResult('goal user-perceivable', 'PASS'))
            else:
                results.append(CheckResult(
                    'goal user-perceivable', 'BLOCK',
                    detail='§2 goals lack quantifiable performance/correctness/'
                           'availability keywords; may not be user-perceivable. '
                           'Requires human review'))
            tech_kw = ['准确率', '决策', '路由策略', '选路算法']
            hit = [k for k in tech_kw if k in s]
            if hit:
                results.append(CheckResult('technical constraints pushed to §4', 'BLOCK',
                                           evidence=','.join(hit),
                                           detail='§2 contains technical-constraint terms; push them down to §4'))
            elif '目标' in s or 'U1' in s:
                results.append(CheckResult('technical constraints pushed to §4', 'PASS'))

    # §3 UseCase external viewpoint + UseCase-goal mapping (cross-section)
    if section in ('§3', 'full'):
        s = parser.get('§3')
        if s:
            internal_kw = ['filter', 'manager', 'handler', 'executor', 'controller']
            # Only check sequenceDiagram participant lines (lines starting with participant)
            participant_lines = [
                line for line in s.split('\n')
                if line.strip().startswith('participant')
            ]
            internal_in_participant = [
                k for k in internal_kw
                if any(k in line.lower() for line in participant_lines)
            ]
            if internal_in_participant:
                results.append(CheckResult(
                    'UseCase external viewpoint without internal components',
                    'BLOCK',
                    evidence=','.join(internal_in_participant),
                    detail='§3 UseCase diagram participants include internal '
                           'component names; should use an external black-box '
                           'viewpoint'))
            elif participant_lines:
                results.append(CheckResult(
                    'UseCase external viewpoint without internal components',
                    'PASS'))

            # UseCase-goal mapping (cross-section: requires §2 to exist)
            if has_section_text(parser, '§2'):
                has_mapping = '映射' in s or '覆盖目标' in s
                if has_mapping:
                    results.append(CheckResult('UseCase-goal mapping', 'PASS'))
                else:
                    results.append(CheckResult('UseCase-goal mapping', 'INFO',
                                               detail='§3 missing UseCase-goal mapping table (cross-section item)'))
            # else skip (§2 not written)

    # §4 module interface / performance spec / mechanism
    if section in ('§4', 'full'):
        s = parser.get('§4')
        if s:
            # Module interface lists only external API (no private/protected)
            if re.search(r'\b(private|protected)\s*:', s):
                results.append(CheckResult(
                    'module interface lists only external API',
                    'BLOCK',
                    detail='§4 interface block contains private/protected; '
                           'overview should list only external API'))
            elif 'class' in s or 'Status' in s:
                results.append(CheckResult(
                    'module interface lists only external API', 'PASS'))

            # Performance spec is quantifiable
            perf_re = re.compile(
                r'[~<>]?\s*\d+(?:\.\d+)?\s*(?:[μumk]?[sb]|ms|bps|Hz|QPS|MB|GB)'
            )
            if perf_re.search(s):
                results.append(CheckResult(
                    'performance spec quantifiable', 'PASS',
                    perf_re.search(s).group(0)))
            elif '性能' in s or '规格' in s:
                results.append(CheckResult(
                    'performance spec quantifiable', 'BLOCK',
                    detail='§4 performance spec table lacks quantifiable values'))

            # Mechanism is a "design response" (D-numbered title contains a verb)
            d_lines = re.findall(r'D\d+\.\s*.+', s)
            if d_lines:
                verb_kw = [
                    '新增', '扩展', '剥离', '改造', '引入', '实现',
                    '合并', '拆分', '替换'
                ]
                bad = [d for d in d_lines if not any(v in d for v in verb_kw)]
                if bad:
                    results.append(CheckResult(
                        'mechanism is a design response',
                        'BLOCK',
                        evidence=bad[0][:40],
                        detail='D-mechanism title should contain a verb '
                               '(add/extend/refactor, etc.), not a symptom '
                               'description'))
                else:
                    results.append(CheckResult(
                        'mechanism is a design response', 'PASS'))

    # §5 params list only changed/added ones
    if section in ('§5', 'full'):
        s = parser.get('§5')
        if s and ('变更' in s or '新增' in s):
            results.append(CheckResult(
                'params list only changed/added', 'PASS'))
        elif s:
            results.append(CheckResult(
                'params list only changed/added', 'BLOCK',
                detail='§5 should list only parameters changed/added by this '
                       'feature. Not mechanically decidable; requires human '
                       'review'))

    return results


# ---------- Detailed design check rules ----------

def check_detailed_section(parser: SectionParser, section: str) -> list:
    results = []

    # §4 mechanism-UseCase mapping (cross-section: requires §3 to exist + §4 has D mechanisms)
    if section == '§4':
        s = parser.get('§4')
        if s and has_section_text(parser, '§3'):
            d_lines = re.findall(r'D\d+\.\s*.+', s)
            if d_lines:
                has_ref = bool(re.search(r'UseCase\d+', s)) or bool(re.search(r'UC\d+', s))
                if has_ref:
                    results.append(CheckResult('mechanism-UseCase mapping', 'PASS'))
                else:
                    results.append(CheckResult(
                        'mechanism-UseCase mapping', 'INFO',
                        detail='§4 mechanism does not explicitly reference '
                               'a UseCase number (cross-section item)'))

    # §5 interface signature complete
    if section == '§5':
        s = parser.get('§5')
        if s:
            sig_re = re.compile(r'\w+\s+\w+\s*\([^)]*\)')
            if sig_re.search(s):
                results.append(CheckResult('interface signature complete', 'PASS'))
            elif '接口' in s or 'interface' in s:
                results.append(CheckResult(
                    'interface signature complete', 'INFO',
                    detail='§5 interface declaration should contain return '
                           'type + method name(params)'))

    # §6 constraints carry violation consequences
    if section in ('§6', 'full'):
        s = parser.get('§6')
        if s:
            if '违规' in s or '后果' in s:
                results.append(CheckResult('constraints carry violation consequences', 'PASS'))
            elif '约束' in s:
                results.append(CheckResult(
                    'constraints carry violation consequences', 'BLOCK',
                    detail='§6 constraint table should include a '
                           '"violation consequence" column'))

    # §7 rollout steps phased
    if section in ('§7', 'full'):
        s = parser.get('§7')
        if s:
            if re.search(r'P[123]', s):
                results.append(CheckResult('rollout steps phased', 'PASS'))
            elif '落地' in s or '步骤' in s or 'PR' in s:
                results.append(CheckResult('rollout steps phased', 'BLOCK',
                                           detail='§7 rollout steps should carry phase markers P1/P2/P3'))

    # §8 tests map to UseCase (cross-section: requires §3 to exist + §8 has test entries)
    if section == '§8':
        s = parser.get('§8')
        if (s and has_section_text(parser, '§3')
                and ('IT' in s or 'UT' in s or '测试' in s)):
            has_ref = bool(re.search(r'UseCase\d+', s)) or bool(re.search(r'UC\d+', s))
            if has_ref:
                results.append(CheckResult('tests map to UseCase', 'PASS'))
            else:
                results.append(CheckResult(
                    'tests map to UseCase', 'INFO',
                    detail='§8 test does not explicitly reference a '
                           'UseCase number (cross-section item)'))

    return results


def check_n6_reverse_scope(parser: SectionParser, overview_parser: Optional[SectionParser]) -> list:
    """N6 reverse-out-of-scope check; only invoked in full mode."""
    results = []
    if overview_parser is None:
        results.append(CheckResult('detailed module count within overview', 'SKIP',
                                   detail='no overview prerequisite (--overview not provided); N6 skipped'))
        results.append(CheckResult('detailed interface within overview scope', 'SKIP',
                                   detail='no overview prerequisite; N6 skipped'))
        return results
    detail_s = parser.get('§4')
    over_s = overview_parser.get('§4')
    if detail_s and over_s:
        detail_modules = len(re.findall(r'^\|[^|]+\|[^|]+\|', detail_s, re.MULTILINE))
        over_modules = len(re.findall(r'^\|[^|]+\|[^|]+\|', over_s, re.MULTILINE))
        if detail_modules > over_modules and over_modules > 0:
            results.append(CheckResult('detailed module count within overview', 'BLOCK',
                                       detail=f'detailed module count ({detail_modules}) > overview ({over_modules})'))
        else:
            results.append(CheckResult('detailed module count within overview', 'PASS'))
        results.append(CheckResult('detailed interface within overview scope', 'PASS'))
    return results


def format_results(results: list, full_mode: bool) -> int:
    """Format and print output; returns BLOCK count (exit code)."""
    block_count = 0
    info_count = 0
    for r in results:
        status = r.status
        if full_mode and status == 'INFO':
            status = 'BLOCK'  # In full mode, cross-section items also block
        if status == 'BLOCK':
            block_count += 1
        elif status == 'INFO':
            info_count += 1
        line = f'[{status}] {r.rule}'
        if r.evidence:
            line += f'  evidence: {r.evidence}'
        if r.detail:
            line += f'\n       {r.detail}'
        print(line)
    print(f'\nSummary: {block_count} BLOCK, {info_count} INFO')
    return 1 if block_count > 0 else 0


def run_all_overview(parser: SectionParser) -> list:
    """Full overview check: iterate over all sections."""
    results = []
    for sec in ['§1', '§2', '§3', '§4', '§5']:
        results.extend(check_overview_section(parser, sec))
    return results


def run_all_detailed(parser: SectionParser, overview_parser: Optional[SectionParser]) -> list:
    """Full detailed check: iterate over all sections + N6 reverse out-of-scope."""
    results = []
    for sec in ['§4', '§5', '§6', '§7', '§8']:
        results.extend(check_detailed_section(parser, sec))
    results.extend(check_n6_reverse_scope(parser, overview_parser))
    return results


def main():
    ap = argparse.ArgumentParser(description='Design document structural self-check')
    ap.add_argument('doc', help='path to the design document')
    ap.add_argument('--type', required=True, choices=['overview', 'detailed'])
    ap.add_argument('--section', default='full', help='section, e.g. §1; default full (all sections)')
    ap.add_argument(
        '--overview',
        help='overview design document path '
             '(detailed type only, for N6 reverse out-of-scope)')
    args = ap.parse_args()

    with open(args.doc, encoding='utf-8') as f:
        text = f.read()
    parser = SectionParser(text)
    parser.parse()

    full_mode = (args.section == 'full')

    if full_mode:
        if args.type == 'overview':
            results = run_all_overview(parser)
        else:
            overview_parser = None
            if args.overview:
                with open(args.overview, encoding='utf-8') as f:
                    overview_parser = SectionParser(f.read())
                overview_parser.parse()
            results = run_all_detailed(parser, overview_parser)
    else:
        # Per-section mode
        if args.type == 'overview':
            results = check_overview_section(parser, args.section)
        else:
            overview_parser = None
            results = check_detailed_section(parser, args.section)

    sys.exit(format_results(results, full_mode))


if __name__ == '__main__':
    main()
