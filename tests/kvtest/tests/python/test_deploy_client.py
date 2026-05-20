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
        self.assertEqual(result[0], {'host': 'h1', 'port': 9000, 'instance_id': 0})
        self.assertEqual(result[1], {'host': 'h2', 'port': 9001, 'instance_id': 1})


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
        self.assertEqual(d._transport(node), 'ssh')

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


if __name__ == '__main__':
    unittest.main()
