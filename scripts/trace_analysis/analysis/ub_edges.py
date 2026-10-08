"""Operation-scoped URMA endpoint evidence and completeness checks."""
from collections import Counter
import re


def trace_operation(trace_id, trace):
    flows = trace.get('flows', {})
    read = any(name.endswith('_GET') and count for name, count in flows.items())
    write = any(name.endswith(('_SET', '_CREATE', '_PUBLISH', '_PUT')) and count for name, count in flows.items())
    if read or write:
        return 'unknown' if read and write else 'read' if read else 'write'
    if re.match(r'^(?:getBuffer|batchGet|Get)-', trace_id, re.I):
        return 'read'
    if re.match(r'^(?:setStringView|createBuffer|publish|batchPut|setBuffer|Put)-', trace_id, re.I):
        return 'write'
    return 'unknown'


def group_operation_edges(traces):
    grouped = {operation: {} for operation in ('read', 'write', 'unknown')}
    for trace_id, trace in traces.items():
        edges = grouped[trace_operation(trace_id, trace)]
        for event in trace.get('ub_events', []):
            if event.get('event_type') != 'total':
                continue
            src, target = event.get('src_addr'), event.get('target_addr')
            if not src or not target:
                continue
            entry = edges.setdefault(f'{src} -> {target}', {'count': 0, 'latencies': []})
            entry['count'] += 1
            if event.get('cost_ms') is not None:
                entry['latencies'].append(event['cost_ms'])
    return grouped


def _raw_endpoints(raw):
    src = re.search(r'\b(?:src|source)\s+(?:addr|address):\s*([^,\s]+)', raw, re.I)
    target = re.search(r'\b(?:tgt|target|dst|destination)\s+(?:addr|address):\s*([^,\s]+)', raw, re.I)
    return (src[1], target[1]) if src and target else None


def _collection_errors(traces):
    errors = []
    for trace_id, trace in traces.items():
        if not isinstance(trace, dict):
            errors.append(f'UB edge {trace_id}: trace must be an object')
            continue
        flows = trace.get('flows', {})
        if not isinstance(flows, dict):
            errors.append(f'UB edge {trace_id}: flows must be an object')
        elif any(not isinstance(name, str) for name in flows):
            errors.append(f'UB edge {trace_id}: flow names must be strings')
        for field in ('ub_events', 'evidence'):
            rows = trace.get(field, [])
            if not isinstance(rows, list):
                errors.append(f'UB edge {trace_id}: {field} must be an array')
                continue
            for row in rows:
                if not isinstance(row, dict):
                    errors.append(f'UB edge {trace_id}: {field} entries must be objects')
                    continue
                for name in ('raw', 'text', 'src_addr', 'target_addr'):
                    if row.get(name) is not None and not isinstance(row[name], str):
                        errors.append(f'UB edge {trace_id}: {name} must be a string')
    return errors


def ub_edge_errors(report):
    errors, total = [], Counter()
    traces = report.get('traces', {})
    shape_errors = _collection_errors(traces)
    if shape_errors:
        return shape_errors
    for trace_id, trace in traces.items():
        events = trace.get('ub_events', [])
        raw_totals = set()
        for event in events:
            if event.get('event_type') != 'total':
                continue
            raw = event.get('raw') or ''
            raw_totals.add(raw)
            expected = _raw_endpoints(raw)
            endpoints = (event.get('src_addr'), event.get('target_addr'))
            if expected and expected != endpoints:
                errors.append(f'UB edge {trace_id}: raw endpoints differ from parsed event')
            if all(endpoints):
                total[' -> '.join(endpoints)] += 1
        for evidence in trace.get('evidence', []):
            raw = evidence.get('text') or evidence.get('raw') or ''
            if '[URMA_ELAPSED_TOTAL]' in raw and _raw_endpoints(raw) and raw not in raw_totals:
                errors.append(f'UB edge {trace_id}: raw TOTAL event missing from model')
    dimensions = report.get('dimensions', {})
    if not isinstance(dimensions, dict):
        return errors + ['UB edge dimensions must be an object']
    summary = dimensions.get('ub_summary', {})
    if not isinstance(summary, dict):
        return errors + ['UB edge summary must be an object']
    if summary.get('edge_operation_schema_version') == 1 and 'edges_by_operation' not in summary:
        errors.append('UB edge operation groups missing from partitioned model')
    edges = summary.get('edges', {})
    if not isinstance(edges, dict):
        return errors + ['UB edge collection must be an object']
    if any(not isinstance(item, dict) for item in edges.values()):
        return errors + ['UB edge entries must be objects']
    actual = {edge: item.get('count') for edge, item in summary.get('edges', {}).items()}
    if dict(total) != actual:
        errors.append('UB edge total counts differ from parsed events')
    if 'edges_by_operation' in summary:
        if not isinstance(summary['edges_by_operation'], dict):
            return errors + ['UB edge operation groups must be an object']
        grouped = group_operation_edges(traces)
        for operation, edges in grouped.items():
            expected = {edge: item['count'] for edge, item in edges.items()}
            operation_edges = summary['edges_by_operation'].get(operation)
            if not isinstance(operation_edges, dict):
                errors.append(f'UB edge {operation} group must be an object')
                continue
            if any(not isinstance(item, dict) for item in operation_edges.values()):
                errors.append(f'UB edge {operation} entries must be objects')
                continue
            actual = {edge: item.get('count') for edge, item in operation_edges.items()}
            if expected != actual:
                errors.append(f'UB edge {operation} counts differ from operation evidence')
    return errors
