#!/usr/bin/env python3

import csv
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from parse_resource import load_resource_csv, render_html


class TestParseResource(unittest.TestCase):
    def test_csv_and_exact_point_payload(self):
        with tempfile.NamedTemporaryFile(mode='w', newline='', delete=False) as f:
            writer = csv.DictWriter(f, fieldnames=[
                'timestamp', 'pid', 'cpu_pct', 'rss_mb',
                'jemalloc_allocated_mb'])
            writer.writeheader()
            writer.writerow({
                'timestamp': '2026-08-31T12:00:00', 'pid': '7',
                'cpu_pct': '12.5', 'rss_mb': '100.25',
                'jemalloc_allocated_mb': '80.125'})
            path = f.name
        try:
            data = load_resource_csv(path)
            self.assertEqual(data['rows'][0]['rss_mb'], 100.25)
            html = render_html(data, 'sample.csv')
            self.assertIn('2026-08-31T12:00:00', html)
            self.assertIn('100.25', html)
            self.assertIn('jemalloc_allocated_mb', html)
            self.assertIn('data-point-index', html)
        finally:
            os.unlink(path)

    def test_empty_metric_is_omitted(self):
        with tempfile.NamedTemporaryFile(mode='w', newline='', delete=False) as f:
            f.write('timestamp,cpu_pct,rss_mb,jemalloc_allocated_mb\n')
            f.write('2026-08-31T12:00:00,1.0,10.0,\n')
            path = f.name
        try:
            data = load_resource_csv(path)
            self.assertNotIn('jemalloc_allocated_mb', data['metrics'])
        finally:
            os.unlink(path)


if __name__ == '__main__':
    unittest.main()
