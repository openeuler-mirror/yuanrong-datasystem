#!/usr/bin/env python3
"""Tests for deploy_pods.py pod-bringup CLI.

Covers the percentage-based distribution added on top of the existing
explicit ``--replicas`` and uniform ``--pods-per-node`` modes: spec parsing,
Largest Remainder rounding, deterministic node-to-bucket assignment, the
refactored ``generate_pod_manifest`` signature, and ``cmd_deploy``'s
distribution wiring with kubectl mocked out. The pure helpers (parse /
distribute) are tested without any kubectl so they are fast and deterministic.
"""

import os
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from deploy_pods import (
    distribute_nodes_by_percentage,
    generate_pod_manifest,
    parse_replicas,
    parse_replicas_pct,
)


def _nodes(n, ip_base=1, name_fmt='node-{:02d}'):
    """Return n sorted nodes [{'ip','name'}]; names are zero-padded so
    lexicographic sort matches numeric sort for n up to 100."""
    return [{'ip': f'10.0.0.{ip_base + i}', 'name': name_fmt.format(i)}
            for i in range(n)]


class TestParseReplicasPct(unittest.TestCase):
    """parse_replicas_pct: PCT:COUNT parsing, types, and malformed-input
    exits. Mirrors parse_replicas's SystemExit-on-malformed contract."""

    def test_happy_path_returns_float_pct_int_count(self):
        self.assertEqual(parse_replicas_pct('30:0,60:1,10:2'),
                         [(30.0, 0), (60.0, 1), (10.0, 2)])

    def test_float_percentages_supported(self):
        self.assertEqual(parse_replicas_pct('33.3:1,66.7:2'),
                         [(33.3, 1), (66.7, 2)])

    def test_whitespace_around_entries_tolerated(self):
        self.assertEqual(parse_replicas_pct(' 30 : 0 , 60 : 1 , 10 : 2 '),
                         [(30.0, 0), (60.0, 1), (10.0, 2)])

    def test_empty_spec_returns_empty_list(self):
        self.assertEqual(parse_replicas_pct(''), [])
        self.assertEqual(parse_replicas_pct(None), [])

    def test_entry_without_colon_exits(self):
        with self.assertRaises(SystemExit):
            parse_replicas_pct('30,60,10')

    def test_non_numeric_percentage_exits(self):
        with self.assertRaises(SystemExit):
            parse_replicas_pct('abc:1,60:1,10:2')

    def test_non_numeric_count_exits(self):
        with self.assertRaises(SystemExit):
            parse_replicas_pct('30:0,60:x,10:2')

    def test_negative_percentage_exits(self):
        with self.assertRaises(SystemExit):
            parse_replicas_pct('-5:1,105:1')

    def test_negative_count_exits(self):
        with self.assertRaises(SystemExit):
            parse_replicas_pct('30:-1,70:1')


class TestDistributeNodesByPercentage(unittest.TestCase):
    """distribute_nodes_by_percentage: Largest Remainder rounding, contiguous
    assignment over caller-sorted nodes, and the sum/empty invariants."""

    def test_perfect_division_no_rounding(self):
        # 10 nodes, 30/60/10 -> 3/6/1 exactly, no remainder to distribute.
        target, summary = distribute_nodes_by_percentage(
            _nodes(10), [(30.0, 0), (60.0, 1), (10.0, 2)])
        self.assertEqual(summary, [(30.0, 0, 3), (60.0, 1, 6), (10.0, 2, 1)])
        # First 3 nodes -> count 0, next 6 -> count 1, last 1 -> count 2.
        counts = [target[n['ip']] for n in _nodes(10)]
        self.assertEqual(counts, [0, 0, 0, 1, 1, 1, 1, 1, 1, 2])

    def test_rounding_uses_largest_remainder(self):
        # 7 nodes, 30/60/10: raw 2.1/4.2/0.7 -> floor 2/4/0=6, leftover 1
        # goes to the largest remainder (0.7 = the 10% bucket) -> 2/4/1=7.
        target, summary = distribute_nodes_by_percentage(
            _nodes(7), [(30.0, 0), (60.0, 1), (10.0, 2)])
        self.assertEqual(summary, [(30.0, 0, 2), (60.0, 1, 4), (10.0, 2, 1)])
        self.assertEqual(len(target), 7)
        # All nodes covered; bucket node counts sum to total nodes exactly.
        self.assertEqual(sum(a for _, _, a in summary), 7)

    def test_tie_break_is_stable_on_spec_order(self):
        # 3 nodes, 50/50: raw 1.5/1.5 -> floor 1/1=2, leftover 1. Both
        # remainders are 0.5 (tie); stable sort keeps spec order, so the
        # FIRST bucket wins the leftover -> 2/1.
        target, summary = distribute_nodes_by_percentage(
            _nodes(3), [(50.0, 1), (50.0, 2)])
        self.assertEqual(summary, [(50.0, 1, 2), (50.0, 2, 1)])

    def test_small_bucket_on_large_cluster(self):
        # 100 nodes, 1%/99%: 1 node gets count 1, 99 get count 0.
        target, summary = distribute_nodes_by_percentage(
            _nodes(100), [(1.0, 1), (99.0, 0)])
        self.assertEqual(summary, [(1.0, 1, 1), (99.0, 0, 99)])
        ones = [ip for ip, c in target.items() if c == 1]
        zeros = [ip for ip, c in target.items() if c == 0]
        self.assertEqual(len(ones), 1)
        self.assertEqual(len(zeros), 99)
        # The single count-1 node is the first (1% bucket gets the first node).
        self.assertEqual(ones[0], '10.0.0.1')

    def test_all_zero_counts_covers_every_node(self):
        # A 0-pod-only plan still assigns every node (with count 0), so the
        # caller can render an empty manifest and report "no pods" cleanly.
        target, summary = distribute_nodes_by_percentage(
            _nodes(4), [(100.0, 0)])
        self.assertEqual(summary, [(100.0, 0, 4)])
        self.assertEqual(set(target.values()), {0})
        self.assertEqual(len(target), 4)

    def test_assignment_respects_input_order(self):
        # The caller-sorted node list drives contiguous bucket assignment;
        # passing a different order changes which IPs land in which bucket.
        nodes = _nodes(4)
        target, _ = distribute_nodes_by_percentage(
            nodes, [(50.0, 1), (50.0, 2)])
        self.assertEqual(target[nodes[0]['ip']], 1)
        self.assertEqual(target[nodes[1]['ip']], 1)
        self.assertEqual(target[nodes[2]['ip']], 2)
        self.assertEqual(target[nodes[3]['ip']], 2)

    def test_sum_not_100_raises(self):
        with self.assertRaisesRegex(ValueError, 'sum to 80'):
            distribute_nodes_by_percentage(_nodes(10), [(50.0, 1), (30.0, 2)])

    def test_sum_within_float_tolerance_ok(self):
        # 33.33 + 33.33 + 33.34 = 100.00; tiny drift must not error.
        distribute_nodes_by_percentage(
            _nodes(3), [(33.33, 1), (33.33, 1), (33.34, 1)])

    def test_empty_nodes_raises(self):
        with self.assertRaisesRegex(ValueError, 'no cluster nodes'):
            distribute_nodes_by_percentage([], [(50.0, 1), (50.0, 2)])

    def test_empty_spec_raises(self):
        with self.assertRaisesRegex(ValueError, 'empty --replicas-pct'):
            distribute_nodes_by_percentage(_nodes(3), [])


class TestGeneratePodManifest(unittest.TestCase):
    """generate_pod_manifest: the refactored signature takes a pre-computed
    target_replicas + ip_to_node and only renders pod specs (no kubectl)."""

    _TEMPLATE = (
        "apiVersion: v1\nkind: Pod\nmetadata:\n  name: podName\n"
        "spec:\n  nodeName: nodeName\n  containers:\n"
        "  - name: containerName\n    image: imageName\n"
        "    resources:\n      limits:\n        cpu: '1'\n        "
        "memory: '1Gi'\n"
    )

    def _config(self, **overrides):
        defaults = dict(image='img:latest', name_prefix='ds-worker',
                        namespace='default', cpu='8', memory='16Gi',
                        requests_cpu='8', requests_memory='16Gi')
        defaults.update(overrides)
        return defaults

    def test_renders_one_pod_per_replica_entry(self):
        target = {'10.0.0.1': 2, '10.0.0.2': 1}
        ip_to_node = {'10.0.0.1': 'node-a', '10.0.0.2': 'node-b'}
        manifest = generate_pod_manifest(self._config(), self._TEMPLATE,
                                        target, ip_to_node)
        # 3 pods total -> 3 documents joined by '---\n'.
        docs = [d for d in manifest.split('---\n') if d.strip()]
        self.assertEqual(len(docs), 3)

    def test_skips_zero_count_entries(self):
        target = {'10.0.0.1': 0, '10.0.0.2': 2}
        ip_to_node = {'10.0.0.1': 'node-a', '10.0.0.2': 'node-b'}
        manifest = generate_pod_manifest(self._config(), self._TEMPLATE,
                                        target, ip_to_node)
        docs = [d for d in manifest.split('---\n') if d.strip()]
        self.assertEqual(len(docs), 2)  # only node-b's 2 pods

    def test_uses_ip_to_node_for_node_name(self):
        target = {'10.0.0.1': 1}
        ip_to_node = {'10.0.0.1': 'node-a'}
        manifest = generate_pod_manifest(self._config(), self._TEMPLATE,
                                        target, ip_to_node)
        self.assertIn('nodeName: node-a', manifest)

    def test_falls_back_to_raw_ip_when_node_name_unknown(self):
        # Degraded-cluster path: discovery failed, ip_to_node is empty.
        # generate_pod_manifest must still render using the raw IP as
        # nodeName rather than crashing.
        target = {'10.0.0.1': 1}
        manifest = generate_pod_manifest(self._config(), self._TEMPLATE,
                                        target, {})
        self.assertIn('nodeName: 10.0.0.1', manifest)

    def test_missing_image_raises(self):
        with self.assertRaisesRegex(ValueError, 'image is required'):
            generate_pod_manifest(self._config(image=''), self._TEMPLATE,
                                  {'10.0.0.1': 1}, {'10.0.0.1': 'n'})

    def test_missing_name_prefix_raises(self):
        with self.assertRaisesRegex(ValueError, 'name_prefix is required'):
            generate_pod_manifest(self._config(name_prefix=''), self._TEMPLATE,
                                  {'10.0.0.1': 1}, {'10.0.0.1': 'n'})


class TestCmdDeployDistribution(unittest.TestCase):
    """cmd_deploy: the four distribution modes are wired through discovery
    and the refactored generate_pod_manifest call. kubectl is mocked so the
    distribution logic is exercised without a cluster."""

    def _args(self, **overrides):
        defaults = dict(namespace='default', prefix='ds-worker',
                        image='img:latest',
                        yaml='config/pod_config.yaml.example',
                        cpu='8', memory='16Gi',
                        requests_cpu='8', requests_memory='16Gi',
                        replicas=None, replicas_pct=None,
                        pods_per_node=None, dry_run=True, force=False,
                        wait=False, timeout=10)
        defaults.update(overrides)
        return SimpleNamespace(**defaults)

    def _template_file(self):
        tf = tempfile.NamedTemporaryFile(mode='w', suffix='.yaml',
                                         delete=False)
        tf.write(TestGeneratePodManifest._TEMPLATE)
        tf.close()
        return tf.name

    @patch('deploy_pods.generate_pod_manifest')
    @patch('deploy_pods.discover_nodes')
    def test_replicas_pct_path_builds_target_from_buckets(self, mock_disc,
                                                           mock_gen):
        mock_disc.return_value = _nodes(10)
        mock_gen.return_value = ''  # dry_run does not inspect the manifest body
        args = self._args(replicas_pct='30:0,60:1,10:2',
                          yaml=self._template_file())
        try:
            from deploy_pods import cmd_deploy
            rc = cmd_deploy(args)
            self.assertEqual(rc, 0)
            mock_gen.assert_called_once()
            call_args = mock_gen.call_args
            target = call_args[0][2]  # 3rd positional: target_replicas
            # 3 nodes count 0, 6 count 1, 1 count 2.
            counts = sorted(target.values())
            self.assertEqual(counts, [0, 0, 0, 1, 1, 1, 1, 1, 1, 2])
        finally:
            os.unlink(args.yaml)

    @patch('deploy_pods.generate_pod_manifest')
    @patch('deploy_pods.discover_nodes')
    def test_replicas_pct_no_nodes_returns_error(self, mock_disc, mock_gen):
        mock_disc.return_value = []
        args = self._args(replicas_pct='30:0,60:1,10:2',
                          yaml=self._template_file())
        try:
            from deploy_pods import cmd_deploy
            rc = cmd_deploy(args)
            self.assertEqual(rc, 1)
            mock_gen.assert_not_called()
        finally:
            os.unlink(args.yaml)

    @patch('deploy_pods.generate_pod_manifest')
    @patch('deploy_pods.discover_nodes')
    def test_explicit_replicas_unknown_ip_returns_error(self, mock_disc,
                                                         mock_gen):
        mock_disc.return_value = _nodes(3)  # 10.0.0.1..3
        args = self._args(replicas='10.0.0.1:2,10.0.0.9:1',
                          yaml=self._template_file())
        try:
            from deploy_pods import cmd_deploy
            rc = cmd_deploy(args)
            self.assertEqual(rc, 1)
            mock_gen.assert_not_called()
        finally:
            os.unlink(args.yaml)

    @patch('deploy_pods.generate_pod_manifest')
    @patch('deploy_pods.discover_nodes')
    def test_pods_per_node_uniform_path(self, mock_disc, mock_gen):
        mock_disc.return_value = _nodes(3)
        mock_gen.return_value = ''
        args = self._args(pods_per_node=2, yaml=self._template_file())
        try:
            from deploy_pods import cmd_deploy
            rc = cmd_deploy(args)
            self.assertEqual(rc, 0)
            target = mock_gen.call_args[0][2]
            self.assertEqual(set(target.values()), {2})
            self.assertEqual(len(target), 3)
        finally:
            os.unlink(args.yaml)

    @patch('deploy_pods.generate_pod_manifest')
    @patch('deploy_pods.discover_nodes')
    def test_default_path_one_per_node(self, mock_disc, mock_gen):
        mock_disc.return_value = _nodes(4)
        mock_gen.return_value = ''
        args = self._args(yaml=self._template_file())  # no distribution flag
        try:
            from deploy_pods import cmd_deploy
            rc = cmd_deploy(args)
            self.assertEqual(rc, 0)
            target = mock_gen.call_args[0][2]
            self.assertEqual(set(target.values()), {1})
            self.assertEqual(len(target), 4)
        finally:
            os.unlink(args.yaml)

    @patch('deploy_pods.generate_pod_manifest')
    @patch('deploy_pods.discover_nodes')
    def test_coordinator_hand_rolled_namespace_without_replicas_pct(self,
                                                                     mock_disc,
                                                                     mock_gen):
        # deploy_coordinator._build_deploy_pods_args builds a SimpleNamespace
        # without replicas_pct; cmd_deploy must tolerate getattr absence and
        # follow the --replicas path (coordinator always passes replicas).
        mock_disc.return_value = _nodes(3)
        mock_gen.return_value = ''
        args = SimpleNamespace(namespace='default', prefix='coordinator-a',
                                image='img:latest',
                                yaml=self._template_file(),
                                cpu='8', memory='16Gi',
                                requests_cpu='8', requests_memory='16Gi',
                                replicas='10.0.0.1:1,10.0.0.2:1,10.0.0.3:1',
                                pods_per_node=None, dry_run=True, force=False,
                                wait=True, timeout=10)
        # Note: no replicas_pct attribute at all.
        try:
            from deploy_pods import cmd_deploy
            rc = cmd_deploy(args)
            self.assertEqual(rc, 0)
            target = mock_gen.call_args[0][2]
            self.assertEqual(target, {'10.0.0.1': 1, '10.0.0.2': 1,
                                      '10.0.0.3': 1})
        finally:
            os.unlink(args.yaml)


if __name__ == '__main__':
    unittest.main()
