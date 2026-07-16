#!/usr/bin/env python3
"""Tests for deploy_client.py pure/logic functions."""

import json
import os
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from deploy_client import Deployer, parse_duration, _parse_pipeline


class TestParseDuration(unittest.TestCase):
    def test_seconds(self):
        self.assertEqual(parse_duration('30s'), 30)

    def test_minutes(self):
        self.assertEqual(parse_duration('5m'), 300)

    def test_hours(self):
        self.assertEqual(parse_duration('2h'), 7200)

    def test_bare_number(self):
        self.assertEqual(parse_duration('60'), 60)

    def test_zero(self):
        self.assertEqual(parse_duration('0'), 0)


class TestParsePipeline(unittest.TestCase):
    def test_comma(self):
        self.assertEqual(_parse_pipeline('set,get,exist'), ['set', 'get', 'exist'])

    def test_empty(self):
        self.assertEqual(_parse_pipeline(''), [])

    def test_single(self):
        self.assertEqual(_parse_pipeline('set'), ['set'])


class _FakeDeployer:
    """Minimal Deployer-like object for testing config generation without files."""
    pass


def _make_deployer(nodes, config_template, transport='ssh'):
    """Create a Deployer with mocked __init__."""
    deploy = {'nodes': nodes, 'transport': transport, 'remote_work_dir': '/tmp/kvtest'}
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json.dump(deploy, f)
        deploy_path = f.name
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json.dump(config_template, f)
        config_path = f.name

    d = Deployer(deploy_path, config_path)
    for p in [deploy_path, config_path]:
        os.unlink(p)
    return d


class TestBuildConfigNodes(unittest.TestCase):
    def test_basic(self):
        nodes = [
            {'host': 'h1', 'port': 9000, 'instance_id': 0},
            {'host': 'h2', 'port': 9001, 'instance_id': 1},
        ]
        d = _make_deployer(nodes, {'listen_port': 9000})
        result = d.build_config_nodes()
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0], {'host': 'h1', 'port': 9000, 'instance_id': 0, 'role': 'writer'})
        self.assertEqual(result[1], {'host': 'h2', 'port': 9001, 'instance_id': 1, 'role': 'writer'})

    def test_with_roles(self):
        nodes = [
            {'host': 'h1', 'port': 9000, 'instance_id': 0, 'role': 'writer'},
            {'host': 'h2', 'port': 9000, 'instance_id': 1, 'role': 'reader'},
        ]
        d = _make_deployer(nodes, {'listen_port': 9000})
        result = d.build_config_nodes()
        self.assertEqual(result[0]['role'], 'writer')
        self.assertEqual(result[1]['role'], 'reader')


class TestBuildPeers(unittest.TestCase):
    def test_auto_exclude_self(self):
        nodes = [
            {'host': 'h1', 'port': 9000, 'instance_id': 0},
            {'host': 'h2', 'port': 9000, 'instance_id': 1},
        ]
        d = _make_deployer(nodes, {'listen_port': 9000})
        peers = d.build_peers(nodes[0])
        self.assertEqual(len(peers), 1)
        self.assertEqual(peers[0], 'http://h2:9000')

    def test_explicit_peers(self):
        nodes = [
            {'host': 'h1', 'port': 9000, 'instance_id': 0,
             'peers': ['http://custom:9000']},
        ]
        d = _make_deployer(nodes, {'listen_port': 9000})
        peers = d.build_peers(nodes[0])
        self.assertEqual(peers, ['http://custom:9000'])


class TestBuildNodeOverrides(unittest.TestCase):
    def test_override_keys(self):
        nodes = [{'host': 'h1', 'port': 9000, 'instance_id': 0,
                  'role': 'reader', 'pipeline': 'getBuffer'}]
        d = _make_deployer(nodes, {'listen_port': 9000})
        overrides = d.build_node_overrides(nodes[0])
        self.assertEqual(overrides['role'], 'reader')
        self.assertEqual(overrides['pipeline'], 'getBuffer')
        self.assertNotIn('host', overrides)
        self.assertNotIn('port', overrides)

    def test_no_overrides(self):
        nodes = [{'host': 'h1', 'port': 9000, 'instance_id': 0}]
        d = _make_deployer(nodes, {'listen_port': 9000})
        overrides = d.build_node_overrides(nodes[0])
        self.assertEqual(overrides, {})


class TestGenerateConfig(unittest.TestCase):
    def test_full(self):
        nodes = [
            {'host': 'h1', 'port': 9000, 'instance_id': 0},
            {'host': 'h2', 'port': 9000, 'instance_id': 1},
        ]
        template = {'etcd_address': 'x:1', 'listen_port': 9000}
        d = _make_deployer(nodes, template)
        config = d.generate_config(nodes[0])
        self.assertEqual(config['instance_id'], 0)
        self.assertEqual(len(config['nodes']), 2)
        self.assertEqual(len(config['peers']), 1)
        self.assertEqual(config['etcd_address'], 'x:1')

    def test_preserves_env(self):
        nodes = [{'host': 'h1', 'port': 9000, 'instance_id': 0}]
        template = {'etcd_address': 'x:1', 'listen_port': 9000,
                    'env': {'FOO': 'bar'}}
        d = _make_deployer(nodes, template)
        config = d.generate_config(nodes[0])
        self.assertEqual(config['env'], {'FOO': 'bar'})


class TestTransport(unittest.TestCase):
    def test_ssh(self):
        d = _make_deployer([{'host': '10.0.0.1', 'port': 9000, 'instance_id': 0}],
                           {'listen_port': 9000})
        node = {'host': '10.0.0.1', 'port': 9000, 'instance_id': 0}
        self.assertEqual(d._transport(node), 'ssh')

    def test_localhost(self):
        d = _make_deployer([{'host': 'localhost', 'port': 9000, 'instance_id': 0}],
                           {'listen_port': 9000}, transport='ssh')
        node = {'host': 'localhost', 'port': 9000, 'instance_id': 0}
        self.assertEqual(d._transport(node), 'localhost')

    def test_kubectl(self):
        d = _make_deployer([{'host': 'pod1', 'port': 9000, 'instance_id': 0,
                              'transport': 'kubectl'}],
                           {'listen_port': 9000})
        node = {'host': 'pod1', 'transport': 'kubectl', 'instance_id': 0}
        self.assertEqual(d._transport(node), 'kubectl')


class TestCommHost(unittest.TestCase):
    def test_explicit(self):
        d = _make_deployer([{'host': 'h1', 'port': 9000, 'instance_id': 0,
                              'comm_host': '10.0.0.1'}],
                           {'listen_port': 9000})
        node = {'host': 'h1', 'comm_host': '10.0.0.1'}
        self.assertEqual(d._comm_host(node), '10.0.0.1')

    def test_kubectl_pod_ip(self):
        d = _make_deployer([{'host': 'pod1', 'port': 9000, 'instance_id': 0,
                              'transport': 'kubectl', 'pod_ip': '10.0.0.5'}],
                           {'listen_port': 9000})
        node = {'host': 'pod1', 'transport': 'kubectl', 'pod_ip': '10.0.0.5'}
        self.assertEqual(d._comm_host(node), '10.0.0.5')

    def test_default_host(self):
        d = _make_deployer([{'host': '192.168.1.10', 'port': 9000, 'instance_id': 0}],
                           {'listen_port': 9000})
        node = {'host': '192.168.1.10'}
        self.assertEqual(d._comm_host(node), '192.168.1.10')


class TestGenConfig(unittest.TestCase):
    """Tests for cmd_gen_config output correctness."""

    def _run_gen_config(self, extra_args):
        """Run gen-config with mocked _get_pods and return (deploy, config) dicts."""
        import argparse
        from deploy_client import cmd_gen_config

        with tempfile.TemporaryDirectory() as tmpdir:
            base_args = [
                '-o', tmpdir,
                '-m', 'benchmark',
                '--test-mode', 'set_local',
                '--worker-memory-mb', '4096',
            ]
            base_args.extend(extra_args)

            import deploy_client as dc
            parser = argparse.ArgumentParser()
            dc._add_gen_config_args(parser)

            args = parser.parse_args(base_args)

            # Mock _get_pods for kubectl tests
            with patch.object(dc, '_get_pods', return_value=[
                {'name': 'pod-0', 'ip': '10.0.0.1', 'node': 'node1'},
                {'name': 'pod-1', 'ip': '10.0.0.2', 'node': 'node1'},
            ]):
                cmd_gen_config(args)

            deploy_path = os.path.join(tmpdir, 'deploy.json')
            config_path = os.path.join(tmpdir, 'config.json')

            deploy = None
            config = None
            if os.path.exists(deploy_path):
                with open(deploy_path) as f:
                    deploy = json.load(f)
            if os.path.exists(config_path):
                with open(config_path) as f:
                    config = json.load(f)
            return deploy, config

    # --- Benchmark mode ---

    def test_benchmark_generates_deploy_json(self):
        """Benchmark mode should generate deploy.json for deployment."""
        deploy, config = self._run_gen_config([
            '--nodes', '127.0.0.1:9000',
        ])
        self.assertIsNotNone(deploy, "benchmark mode should generate deploy.json")
        self.assertIn('nodes', deploy)
        self.assertEqual(len(deploy['nodes']), 1)

    def test_benchmark_config_no_listen_port(self):
        """Benchmark config should NOT contain listen_port."""
        deploy, config = self._run_gen_config([
            '--nodes', '127.0.0.1:9000',
        ])
        self.assertIsNotNone(config)
        self.assertNotIn('listen_port', config)

    def test_benchmark_config_has_cleanup_method(self):
        """Benchmark config should contain cleanup_method."""
        deploy, config = self._run_gen_config([
            '--nodes', '127.0.0.1:9000',
        ])
        self.assertIsNotNone(config)
        self.assertIn('cleanup_method', config)
        self.assertEqual(config['cleanup_method'], 'del')

    def test_benchmark_config_has_cluster_name(self):
        """Benchmark config should always contain cluster_name."""
        deploy, config = self._run_gen_config([
            '--nodes', '127.0.0.1:9000',
        ])
        self.assertIsNotNone(config)
        self.assertIn('cluster_name', config)

    def test_benchmark_with_multiple_nodes(self):
        """Benchmark with --nodes "h1:p1,h2:p2" generates multi-node deploy.json."""
        deploy, config = self._run_gen_config([
            '--nodes', '1.2.3.4:9000,5.6.7.8:9001',
        ])
        self.assertIsNotNone(deploy)
        self.assertEqual(len(deploy['nodes']), 2)
        self.assertEqual(deploy['nodes'][0]['host'], '1.2.3.4')
        self.assertEqual(deploy['nodes'][1]['host'], '5.6.7.8')

    def test_benchmark_default_localhost(self):
        """Benchmark without --nodes generates localhost deploy.json."""
        deploy, config = self._run_gen_config([])
        self.assertIsNotNone(deploy)
        self.assertEqual(len(deploy['nodes']), 1)
        self.assertEqual(deploy['nodes'][0]['host'], 'localhost')

    def test_benchmark_cleanup_ttl(self):
        """Benchmark with --cleanup-method ttl sets it in config."""
        deploy, config = self._run_gen_config([
            '--nodes', '127.0.0.1:9000',
            '--cleanup-method', 'ttl',
            '--ttl', '5',
        ])
        self.assertEqual(config['cleanup_method'], 'ttl')
        self.assertIn('set_param', config)
        self.assertEqual(config['set_param']['ttl_second'], 5)

    # --- Pipeline mode with kubectl ---

    def test_pipeline_generates_deploy_json(self):
        """Pipeline mode with --prefix generates deploy.json with kubectl transport."""
        deploy, config = self._run_gen_config([
            '-m', 'pipeline',
            '-p', 'ds-worker',
            '-n', 'datasystem',
        ])
        self.assertIsNotNone(deploy)
        self.assertEqual(deploy['transport'], 'kubectl')
        self.assertIn('nodes', deploy)

    def test_pipeline_config_has_cluster_name(self):
        """Pipeline config should always contain cluster_name."""
        deploy, config = self._run_gen_config([
            '-m', 'pipeline',
            '-p', 'ds-worker',
            '-n', 'datasystem',
        ])
        self.assertIsNotNone(config)
        self.assertIn('cluster_name', config)

    def test_nodes_with_writer_count(self):
        """Pipeline with --nodes and --writer-count assigns roles correctly."""
        deploy, config = self._run_gen_config([
            '-m', 'pipeline',
            '--nodes', '1.2.3.4:9000,5.6.7.8:9001',
            '-w', '1',
        ])
        self.assertIsNotNone(deploy)
        self.assertEqual(deploy['nodes'][0]['role'], 'writer')
        self.assertEqual(deploy['nodes'][1]['role'], 'reader')

    # --- Data verification (verify block) ---

    def test_verify_default_omitted(self):
        """Default verify args must NOT emit a verify block (legacy baseline)."""
        deploy, config = self._run_gen_config([
            '-m', 'pipeline',
            '--nodes', '127.0.0.1:9000',
        ])
        self.assertIsNotNone(config)
        self.assertNotIn('verify', config)

    def test_verify_level_full_emitted(self):
        """--verify-level full emits verify.level (and only that key)."""
        deploy, config = self._run_gen_config([
            '-m', 'pipeline',
            '--nodes', '127.0.0.1:9000',
            '--verify-level', 'full',
        ])
        self.assertIsNotNone(config)
        self.assertEqual(config['verify'], {'level': 'full'})

    def test_verify_sample_with_fail_op(self):
        """--verify-level sample + --verify-fail-op emits both keys."""
        deploy, config = self._run_gen_config([
            '-m', 'pipeline',
            '--nodes', '127.0.0.1:9000',
            '--verify-level', 'sample',
            '--verify-fail-op',
        ])
        self.assertIsNotNone(config)
        self.assertEqual(config['verify']['level'], 'sample')
        self.assertTrue(config['verify']['fail_op'])

    def test_verify_sample_sizes_emitted(self):
        """Custom sample_bytes/sample_step are emitted; defaults are omitted."""
        deploy, config = self._run_gen_config([
            '-m', 'pipeline',
            '--nodes', '127.0.0.1:9000',
            '--verify-level', 'sample',
            '--verify-sample-bytes', '8KB',
            '--verify-sample-step', '512KB',
        ])
        self.assertIsNotNone(config)
        v = config['verify']
        self.assertEqual(v['level'], 'sample')
        self.assertEqual(v['sample_bytes'], '8KB')
        self.assertEqual(v['sample_step'], '512KB')
        self.assertNotIn('fail_op', v)

    def test_verify_off_emitted(self):
        """--verify-level off is explicitly emitted (differs from default size)."""
        deploy, config = self._run_gen_config([
            '-m', 'cache',
            '--nodes', '127.0.0.1:9000',
            '--key-pool-size', '50',
            '--verify-level', 'off',
        ])
        self.assertIsNotNone(config)
        self.assertEqual(config['verify'], {'level': 'off'})

    def test_verify_fail_op_alone_emitted(self):
        """--verify-fail-op alone emits only fail_op (level stays default size)."""
        deploy, config = self._run_gen_config([
            '-m', 'pipeline',
            '--nodes', '127.0.0.1:9000',
            '--verify-fail-op',
        ])
        self.assertIsNotNone(config)
        self.assertEqual(config['verify'], {'fail_op': True})

    def test_verify_invalid_level_rejected(self):
        """argparse choices must reject an unknown verify level."""
        import argparse
        from deploy_client import _add_gen_config_args
        parser = argparse.ArgumentParser()
        _add_gen_config_args(parser)
        with self.assertRaises(SystemExit):
            parser.parse_args(['--verify-level', 'strict'])

    # --- Connect options: enable_local_cache ---

    def test_enable_local_cache_default_true(self):
        """gen-config should emit enable_local_cache=true by default (flag omitted)."""
        deploy, config = self._run_gen_config([
            '--nodes', '127.0.0.1:9000',
        ])
        self.assertIsNotNone(config)
        self.assertIn('connect_options', config)
        self.assertTrue(config['connect_options']['enable_local_cache'])

    def test_enable_local_cache_bare_flag_true(self):
        """Bare --enable-local-cache (no value) should emit enable_local_cache=true."""
        deploy, config = self._run_gen_config([
            '--nodes', '127.0.0.1:9000',
            '--enable-local-cache',
        ])
        self.assertIsNotNone(config)
        self.assertTrue(config['connect_options']['enable_local_cache'])

    def test_enable_local_cache_explicit_true(self):
        """--enable-local-cache true should emit enable_local_cache=true."""
        deploy, config = self._run_gen_config([
            '--nodes', '127.0.0.1:9000',
            '--enable-local-cache', 'true',
        ])
        self.assertIsNotNone(config)
        self.assertTrue(config['connect_options']['enable_local_cache'])

    def test_enable_local_cache_false(self):
        """--enable-local-cache false should emit enable_local_cache=false."""
        deploy, config = self._run_gen_config([
            '--nodes', '127.0.0.1:9000',
            '--enable-local-cache', 'false',
        ])
        self.assertIsNotNone(config)
        self.assertIn('connect_options', config)
        self.assertFalse(config['connect_options']['enable_local_cache'])

    def test_enable_local_cache_invalid_rejected(self):
        """argparse must reject an unknown boolean value for --enable-local-cache."""
        import argparse
        from deploy_client import _add_gen_config_args
        parser = argparse.ArgumentParser()
        _add_gen_config_args(parser)
        with self.assertRaises(SystemExit):
            parser.parse_args(['--enable-local-cache', 'maybe'])

    # --- Runtime environment: --use-brpc ---

    def test_use_brpc_default_no_env(self):
        """Without --use-brpc, config must NOT contain an env block."""
        deploy, config = self._run_gen_config([
            '--nodes', '127.0.0.1:9000',
        ])
        self.assertIsNotNone(config)
        self.assertNotIn('env', config)

    def test_use_brpc_emits_env(self):
        """--use-brpc should emit env.DATASYSTEM_USE_BRPC=true in config."""
        deploy, config = self._run_gen_config([
            '--nodes', '127.0.0.1:9000',
            '--use-brpc',
        ])
        self.assertIsNotNone(config)
        self.assertIn('env', config)
        self.assertEqual(config['env'].get('DATASYSTEM_USE_BRPC'), 'true')

    # --- Service discovery address: --coordinator-address vs --etcd-address ---

    def test_default_etcd_address(self):
        """Without --coordinator-address, config should contain the default etcd_address."""
        deploy, config = self._run_gen_config([
            '--nodes', '127.0.0.1:9000',
        ])
        self.assertIsNotNone(config)
        self.assertEqual(config.get('etcd_address'), '127.0.0.1:2379')
        self.assertNotIn('coordinator_address', config)

    def test_etcd_address_override(self):
        """--etcd-address (without coordinator) overrides etcd_address."""
        deploy, config = self._run_gen_config([
            '--nodes', '127.0.0.1:9000',
            '--etcd-address', '10.0.0.1:2379',
        ])
        self.assertIsNotNone(config)
        self.assertEqual(config.get('etcd_address'), '10.0.0.1:2379')
        self.assertNotIn('coordinator_address', config)

    def test_coordinator_address_emits_coordinator_only(self):
        """--coordinator-address should emit coordinator_address and omit etcd_address."""
        deploy, config = self._run_gen_config([
            '--nodes', '127.0.0.1:9000',
            '--coordinator-address', '127.0.0.1:31511',
        ])
        self.assertIsNotNone(config)
        self.assertEqual(config.get('coordinator_address'), '127.0.0.1:31511')
        self.assertNotIn('etcd_address', config)

    def test_coordinator_address_overrides_etcd(self):
        """--coordinator-address takes priority: --etcd-address is ignored entirely."""
        deploy, config = self._run_gen_config([
            '--nodes', '127.0.0.1:9000',
            '--etcd-address', '10.0.0.1:2379',
            '--coordinator-address', '127.0.0.1:31511',
        ])
        self.assertIsNotNone(config)
        self.assertEqual(config.get('coordinator_address'), '127.0.0.1:31511')
        self.assertNotIn('etcd_address', config)


if __name__ == '__main__':
    unittest.main()
