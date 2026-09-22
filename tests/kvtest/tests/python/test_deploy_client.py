#!/usr/bin/env python3
"""Tests for deploy_client.py pure/logic functions."""

import json
import os
import subprocess
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch, MagicMock

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


class TestSdkOnlyCollect(unittest.TestCase):
    def test_sdk_only_archive_excludes_outputs_and_configs(self):
        from log_collect import REMOTE_ARCHIVE, receive_archive
        for patterns in ([], ['*access*.log', '*access*.gz']):
            with self.subTest(patterns=patterns), tempfile.TemporaryDirectory() as tmp:
                sdk = os.path.join(tmp, 'sdk')
                output = os.path.join(tmp, 'collected')
                os.mkdir(sdk)
                for name in ('access.log', 'access.1.gz', 'client.log'):
                    with open(os.path.join(sdk, name), 'wb') as f:
                        f.write(name.encode())
                d = _make_deployer([{'host': 'localhost', 'instance_id': 0}], {})
                def archive(sources, filters, **kwargs):
                    if sources:
                        self.assertEqual([entry[0] for entry in sources], ['sdk'])
                    return json.dumps(dict(sources=sources, **filters))
                def receive(command, local_dir, **kwargs):
                    return receive_archive([sys.executable, '-c', REMOTE_ARCHIVE, command], local_dir)
                with patch('deploy_client.archive_command', side_effect=archive), \
                        patch('deploy_client.receive_archive', side_effect=receive), \
                        patch('deploy_client.copy_case_files') as configs, \
                        patch.object(d, 'run_on') as run:
                    result = d.do_collect(sdk, output, sdk_only=True,
                                          filters=dict(patterns=patterns, keywords=[], uncompressed_only=False))
                self.assertEqual(result, 0)
                configs.assert_not_called()
                run.assert_not_called()
                actual = os.listdir(os.path.join(output, 'localhost_0', 'sdk'))
                self.assertEqual(set(actual), {'access.log', 'access.1.gz'} if patterns
                                 else {'access.log', 'client.log'})

    def test_sdk_only_transfer_failure_returns_failure(self):
        d = _make_deployer([{'host': 'localhost', 'instance_id': 0}], {})
        with tempfile.TemporaryDirectory() as out, \
                patch('deploy_client.receive_archive', side_effect=RuntimeError('transfer failed')):
            self.assertEqual(d.do_collect('/logs', out, sdk_only=True), 1)

    def test_cli_log_pattern_alias_combines_with_file_pattern(self):
        from deploy_client import main
        d = _make_deployer([], {})
        args = ['deploy_client.py', 'collect', 'deploy.json', '--sdk-only',
                '--log-pattern', '*access*.log', '--file-pattern', '*access*.gz']
        with patch.object(sys, 'argv', args), patch('deploy_client.Deployer', return_value=d), \
                patch.object(d, 'do_collect', return_value=0) as collect:
            main()
        self.assertTrue(collect.call_args.kwargs['sdk_only'])
        self.assertEqual(collect.call_args.kwargs['filters']['patterns'], ['*access*.log', '*access*.gz'])


class TestCudaStartupWait(unittest.TestCase):
    def test_launcher_budget_includes_node_wait(self):
        for wait in (0, 60):
            with self.subTest(wait=wait):
                node = {'pod_name': 'test-client', 'instance_id': 0, 'host_ip': '192.0.2.1',
                        'cuda': {'client_init_wait_seconds': wait}}
                d = _make_deployer([node], {'mode': 'pipeline', 'listen_port': 9000,
                                            'cuda': {'client_init_wait_seconds': 10}}, transport='kubectl')
                d.start_timeout = 60
                d.binary_path = '/tmp/kvtest/kvtest'
                d.scp_to = MagicMock()
                launches = []

                def run_on(node, cmd, check=True, timeout=60, allow_timeout=False):
                    if cmd == 'pgrep -x kvtest':
                        return subprocess.CompletedProcess([], 1, '', '')
                    if '--ready-timeout' in cmd:
                        launches.append((cmd, timeout))
                        return subprocess.CompletedProcess([], 0, '123 0.1\n', '')
                    return subprocess.CompletedProcess([], 0, '', '')

                d.run_on = run_on
                ok, _ = d.start_node(node)
                self.assertTrue(ok)
                self.assertEqual(len(launches), 1)
                self.assertIn('--ready-timeout ' + str(60 + wait), launches[0][0])
                self.assertEqual(launches[0][1], 70 + wait)


class TestStartKeepRemoteConfig(unittest.TestCase):
    """Tests for start_node(keep_remote_config=...) — rolling-upgrade path."""

    def _make_deployer(self, node, template=None):
        d = _make_deployer([node], template or {'mode': 'pipeline', 'listen_port': 9000},
                           transport='kubectl')
        d.start_timeout = 5
        d.binary_path = '/tmp/kvtest/kvtest'
        return d

    def test_keep_remote_config_skips_scp_and_preflight_ok(self):
        """keep_remote_config=True: no scp_to for config, reuses remote config."""
        node = {'pod_name': 'c-0', 'instance_id': 0, 'host_ip': '192.0.2.1',
                'port': 9000, 'role': 'writer'}
        d = self._make_deployer(node, {'mode': 'pipeline', 'listen_port': 9000,
                                       'host_id_env_name': 'HOST_IP'})
        scp_calls = []
        d.scp_to = MagicMock(side_effect=lambda n, s, dst: scp_calls.append((s, dst)))
        launches = []

        def run_on(node, cmd, check=True, timeout=60, allow_timeout=False):
            if cmd == 'pgrep -x kvtest':
                return subprocess.CompletedProcess([], 1, '', '')
            if 'test -f' in cmd and 'config_' in cmd:
                return subprocess.CompletedProcess([], 0, '', '')  # remote config exists
            if 'test -x' in cmd:
                return subprocess.CompletedProcess([], 0, '', '')  # binary exists
            if '--ready-timeout' in cmd:
                launches.append(cmd)
                return subprocess.CompletedProcess([], 0, '123 0.1\n', '')
            return subprocess.CompletedProcess([], 0, '', '')

        d.run_on = run_on
        ok, _ = d.start_node(node, keep_remote_config=True)
        self.assertTrue(ok)
        # No config upload happened (binary/launcher uploads are in install phase, not here)
        for src, dst in scp_calls:
            self.assertNotIn('config_0', dst)
        # Launch happened with the remote config filename
        self.assertTrue(any('config_0.json' in c for c in launches))

    def test_keep_remote_config_fails_when_remote_config_missing(self):
        """keep_remote_config=True with missing remote config: clear error, no launch."""
        node = {'pod_name': 'c-0', 'instance_id': 0, 'host_ip': '192.0.2.1',
                'port': 9000, 'role': 'writer'}
        d = self._make_deployer(node)
        d.scp_to = MagicMock()
        launches = []

        def run_on(node, cmd, check=True, timeout=60, allow_timeout=False):
            if cmd == 'pgrep -x kvtest':
                return subprocess.CompletedProcess([], 1, '', '')
            if 'test -f' in cmd and 'config_' in cmd:
                return subprocess.CompletedProcess([], 1, '', '')  # remote config MISSING
            if '--ready-timeout' in cmd:
                launches.append(cmd)
                return subprocess.CompletedProcess([], 0, '123 0.1\n', '')
            return subprocess.CompletedProcess([], 0, '', '')

        d.run_on = run_on
        ok, elapsed = d.start_node(node, keep_remote_config=True)
        self.assertFalse(ok)
        self.assertEqual(elapsed, 0.0)
        self.assertEqual(launches, [])  # No launch attempted

    def test_keep_remote_config_still_injects_env_from_template(self):
        """keep_remote_config=True reads env block + host_id_env_name from template."""
        node = {'pod_name': 'c-0', 'instance_id': 0, 'host_ip': '192.0.2.1',
                'port': 9000, 'role': 'writer'}
        template = {'mode': 'pipeline', 'listen_port': 9000,
                    'env': {'FOO': 'bar', 'DATASYSTEM_UB_GET_DATA_SIZE_BYTES': '5242880'},
                    'host_id_env_name': 'HOST_IP'}
        d = self._make_deployer(node, template)
        d.scp_to = MagicMock()
        launch_cmds = []

        def run_on(node, cmd, check=True, timeout=60, allow_timeout=False):
            if cmd == 'pgrep -x kvtest':
                return subprocess.CompletedProcess([], 1, '', '')
            if 'test -f' in cmd and 'config_' in cmd:
                return subprocess.CompletedProcess([], 0, '', '')
            if 'test -x' in cmd:
                return subprocess.CompletedProcess([], 0, '', '')
            if '--ready-timeout' in cmd:
                launch_cmds.append(cmd)
                return subprocess.CompletedProcess([], 0, '123 0.1\n', '')
            return subprocess.CompletedProcess([], 0, '', '')

        d.run_on = run_on
        ok, _ = d.start_node(node, keep_remote_config=True)
        self.assertTrue(ok)
        self.assertEqual(len(launch_cmds), 1)
        # FOO from template env, HOST_IP from host_ip, DATASYSTEM_UB_GET_DATA_SIZE_BYTES
        # is NOT re-injected because it's already in custom_env from template.
        self.assertIn('FOO=bar', launch_cmds[0])
        self.assertIn('HOST_IP=192.0.2.1', launch_cmds[0])
        self.assertIn('DATASYSTEM_UB_GET_DATA_SIZE_BYTES=5242880', launch_cmds[0])

    def test_default_regenerates_and_uploads_config(self):
        """keep_remote_config=False (explicit): generate_config + scp_to called."""
        node = {'pod_name': 'c-0', 'instance_id': 0, 'host_ip': '192.0.2.1',
                'port': 9000, 'role': 'writer'}
        d = self._make_deployer(node, {'mode': 'pipeline', 'listen_port': 9000})
        scp_calls = []
        d.scp_to = MagicMock(side_effect=lambda n, s, dst: scp_calls.append(dst))

        def run_on(node, cmd, check=True, timeout=60, allow_timeout=False):
            if cmd == 'pgrep -x kvtest':
                return subprocess.CompletedProcess([], 1, '', '')
            if 'test -x' in cmd:
                return subprocess.CompletedProcess([], 0, '', '')
            if '--ready-timeout' in cmd:
                return subprocess.CompletedProcess([], 0, '123 0.1\n', '')
            return subprocess.CompletedProcess([], 0, '', '')

        d.run_on = run_on
        ok, _ = d.start_node(node, keep_remote_config=False)
        self.assertTrue(ok)
        self.assertTrue(any('config_0' in dst for dst in scp_calls))



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

    def test_cuda_pipeline_options(self):
        _, config = self._run_gen_config([
            '-m', 'pipeline', '--nodes', '127.0.0.1:9000',
            '--cuda-transfer', '--cuda-pin', 'false', '--cuda-device-id', '1',
            '--cuda-runtime-library', '/opt/cuda/libcudart.so',
            '--cuda-client-init-wait-seconds', '60',
            '--pipeline', 'mCreate,mD2h,mSet', '--notify-pipeline', 'mGet,mH2d',
            '--batch-keys-count', '8',
        ])
        self.assertEqual(config['cuda'], {'transfer_enabled': True, 'pin': False,
                                         'device_id': 1, 'runtime_library': '/opt/cuda/libcudart.so',
                                         'client_init_wait_seconds': 60})
        self.assertEqual(config['pipeline'], ['mCreate', 'mD2h', 'mSet'])
        self.assertEqual(config['notify_pipeline'], ['mGet', 'mH2d'])

    def test_cuda_default_and_node_override(self):
        _, config = self._run_gen_config(['-m', 'pipeline', '--nodes', '127.0.0.1:9000'])
        self.assertFalse(config['cuda']['transfer_enabled'])
        self.assertTrue(config['cuda']['pin'])
        self.assertEqual(config['cuda']['client_init_wait_seconds'], 0)
        node = {'host': 'pod-gpu', 'instance_id': 0, 'cuda': {'pin': False}}
        deployer = _make_deployer([node], config, transport='kubectl')
        generated = deployer.generate_config(node)
        self.assertFalse(generated['cuda']['pin'])
        self.assertFalse(generated['cuda']['transfer_enabled'])
        self.assertTrue(config['cuda']['pin'])

    def test_cuda_not_emitted_for_benchmark(self):
        _, config = self._run_gen_config(['--nodes', '127.0.0.1:9000'])
        self.assertNotIn('cuda', config)

    def test_cuda_negative_init_wait_rejected(self):
        with self.assertRaises(SystemExit):
            self._run_gen_config(['-m', 'pipeline', '--nodes', '127.0.0.1:9000',
                                  '--cuda-client-init-wait-seconds', '-1'])

    def test_cuda_transfer_rejected_for_benchmark(self):
        with self.assertRaises(SystemExit):
            self._run_gen_config(['--nodes', '127.0.0.1:9000', '--cuda-transfer'])

    def _run_gen_config(self, extra_args):
        """Run gen-config with mocked get_pods and return (deploy, config) dicts."""
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

            # Mock get_pods (imported into deploy_client from deploy_common)
            # for kubectl-discovery tests. node + host_ip are needed by
            # cmd_gen_config's per-node dict construction.
            with patch.object(dc, 'get_pods', return_value=[
                {'name': 'pod-0', 'ip': '10.0.0.1', 'node': 'node1',
                 'host_ip': '10.0.0.1'},
                {'name': 'pod-1', 'ip': '10.0.0.2', 'node': 'node1',
                 'host_ip': '10.0.0.1'},
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

    def test_default_thread_counts_are_emitted(self):
        """gen-config should emit the default write and total thread counts."""
        _, config = self._run_gen_config([
            '-m', 'pipeline',
            '--nodes', '127.0.0.1:9000',
        ])
        self.assertEqual(config['num_threads'], 4)
        self.assertEqual(config['num_total_threads'], 16)

    def test_benchmark_omits_pipeline_total_threads(self):
        """Benchmark config should use its thread count without a Pipeline total."""
        _, config = self._run_gen_config([
            '--nodes', '127.0.0.1:9000',
        ])
        self.assertEqual(config['num_threads'], 4)
        self.assertNotIn('num_total_threads', config)

    def test_benchmark_emits_client_concurrency(self):
        """Benchmark config should separate Client count from threads per Client."""
        _, config = self._run_gen_config([
            '--nodes', '127.0.0.1:9000',
            '--num-clients', '3',
            '--num-threads', '8',
        ])
        self.assertEqual(config['num_clients'], 3)
        self.assertEqual(config['num_threads'], 8)

    def test_pipeline_write_only_doubles_total_threads(self):
        """An explicit Pipeline write count should derive twice as many total threads."""
        _, config = self._run_gen_config([
            '-m', 'pipeline',
            '--nodes', '127.0.0.1:9000',
            '--num-threads', '16',
        ])
        self.assertEqual(config['num_threads'], 16)
        self.assertEqual(config['num_total_threads'], 32)

    def test_pipeline_rejects_explicit_total_not_greater_than_write(self):
        """Explicit invalid Pipeline thread counts should fail during config generation."""
        with self.assertRaises(SystemExit):
            self._run_gen_config([
                '-m', 'pipeline',
                '--nodes', '127.0.0.1:9000',
                '--num-threads', '16',
                '--num-total-threads', '16',
            ])

    def test_num_total_threads_is_emitted(self):
        """gen-config should preserve the configured read/write concurrency budget."""
        _, config = self._run_gen_config([
            '-m', 'pipeline',
            '--nodes', '127.0.0.1:9000',
            '--num-threads', '2',
            '--num-total-threads', '5',
        ])
        self.assertEqual(config['num_threads'], 2)
        self.assertEqual(config['num_total_threads'], 5)

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

    def test_enable_local_cache_default_false(self):
        """gen-config should emit enable_local_cache=false by default (flag omitted)."""
        deploy, config = self._run_gen_config([
            '--nodes', '127.0.0.1:9000',
        ])
        self.assertIsNotNone(config)
        self.assertIn('connect_options', config)
        self.assertFalse(config['connect_options']['enable_local_cache'])

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

    def test_data_placement_policy_default_emitted(self):
        """gen-config should emit the SDK default write placement policy."""
        _, config = self._run_gen_config([
            '--nodes', '127.0.0.1:9000',
        ])
        self.assertEqual(
            config['connect_options']['data_placement_policy'],
            'PREFERRED_META_OWNER')

    def test_data_placement_policy_override_emitted(self):
        """gen-config should emit an explicitly selected write placement policy."""
        _, config = self._run_gen_config([
            '--nodes', '127.0.0.1:9000',
            '--data-placement-policy', 'PREFERRED_META_OWNER',
        ])
        self.assertEqual(
            config['connect_options']['data_placement_policy'],
            'PREFERRED_META_OWNER')

    def test_data_placement_policy_invalid_rejected(self):
        """argparse choices must reject an unknown write placement policy."""
        import argparse
        from deploy_client import _add_gen_config_args
        parser = argparse.ArgumentParser()
        _add_gen_config_args(parser)
        with self.assertRaises(SystemExit):
            parser.parse_args(['--data-placement-policy', 'INVALID'])

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


class TestDoCleanLogs(unittest.TestCase):
    """do_clean_logs: kill processes + remove run-time output, but preserve
    the install-phase artifacts (kvtest binary, lib/, procmon.py,
    standalone_launcher.py) so a re-deploy skips the ~100MB upload on
    large clusters. Mirrors do_clean's two-phase kill (TERM then -9) so a
    still-running binary doesn't race the rm on its own output files."""

    def _make_deployer(self, remote_work_dir='/tmp/kvtest', nodes=None):
        """Build a Deployer without touching disk: config_template is empty,
        nodes come from deploy.json, and binary_path / VERSION are skipped."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json',
                                         delete=False) as tf:
            json.dump({
                'nodes': nodes or [{'host': '127.0.0.1', 'port': 9000}],
                'remote_work_dir': remote_work_dir,
            }, tf)
            deploy_json = tf.name
        try:
            d = Deployer.__new__(Deployer)
            d.deploy = json.load(open(deploy_json))
            d.config_template = {}
            d.base_dir = os.path.dirname(os.path.abspath(deploy_json))
            d.nodes = d.deploy.get('nodes', [])
            d.remote_work_dir = d.deploy.get('remote_work_dir', '')
            d.binary_path = None
            d.version = '?'
            d.default_transport = d.deploy.get('transport', 'ssh')
            d.default_ssh_user = d.deploy.get('ssh_user', 'root')
            d.ssh_options = d.deploy.get('ssh_options', '')
            d.enable_procmon = d.deploy.get('enable_procmon', False)
            d.listen_port = 9000
            d._host_locks = {}
            return d
        finally:
            os.unlink(deploy_json)

    def _node(self):
        return {'host': '127.0.0.1', 'port': 9000, 'instance_id': '0'}

    @patch('deploy_client.time.sleep')  # skip the real 1s sleeps in the kill phases
    def test_preserves_binary_and_removes_run_time_output(self, mock_sleep):
        # The single rm command must target ONLY run-time products
        # (config_*.json, run.log, metrics_*, resource_monitor.csv, SDK logs)
        # and must NOT match the install artifacts (kvtest, lib/, procmon.py,
        # standalone_launcher.py). A glob regression here would silently turn
        # clean-logs into clean and re-trigger the 100M+ upload.
        d = self._make_deployer(remote_work_dir='/tmp/kvtest')
        cmds = []

        def fake_run_on(node, cmd, check=True, timeout=60, allow_timeout=False):
            cmds.append(cmd)
            # The verify step runs `test -x {remote_work_dir}/kvtest`; return
            # rc=0 so the "binary preserved" branch is taken (the OK path).
            if cmd.startswith('test -x '):
                return subprocess.CompletedProcess(args=[], returncode=0,
                                                    stdout='', stderr='')
            return subprocess.CompletedProcess(args=[], returncode=0,
                                                stdout='', stderr='')
        d.run_on = fake_run_on

        d.do_clean_logs()

        # Find the single rm -rf command issued (kill commands don't contain rm)
        rm_cmds = [c for c in cmds if c.startswith('rm -rf ')]
        self.assertEqual(len(rm_cmds), 1)
        rm = rm_cmds[0]
        # Run-time products must be in the rm target list
        self.assertIn('/tmp/kvtest/config_*.json', rm)
        self.assertIn('/tmp/kvtest/run.log', rm)
        self.assertIn('/tmp/kvtest/metrics_*', rm)
        self.assertIn('/tmp/kvtest/resource_monitor.csv', rm)
        self.assertIn('/root/.datasystem/logs/', rm)
        # Install artifacts must NOT be removed -- the whole point of clean-logs.
        # Check the rm target list (split by space) does not contain the bare
        # remote_work_dir or any install artifact as a standalone token; the
        # run-time globs (config_*.json etc.) legitimately share the dir prefix.
        rm_tokens = rm.split()
        self.assertNotIn('/tmp/kvtest', rm_tokens)
        self.assertNotIn('/tmp/kvtest/', rm_tokens)
        self.assertNotIn('/tmp/kvtest/kvtest', rm_tokens)
        self.assertNotIn('/tmp/kvtest/lib', rm_tokens)
        self.assertNotIn('/tmp/kvtest/lib/', rm_tokens)
        self.assertNotIn('/tmp/kvtest/procmon.py', rm_tokens)
        self.assertNotIn('/tmp/kvtest/standalone_launcher.py', rm_tokens)

    @patch('deploy_client.time.sleep')
    def test_two_phase_kill_then_rm_then_verify(self, mock_sleep):
        # Order matters: kill (TERM) -> kill -9 -> rm run-time output ->
        # verify binary survived. A still-running binary would otherwise keep
        # writing to run.log / metrics_* while we're deleting them.
        d = self._make_deployer()
        cmds = []

        def fake_run_on(node, cmd, check=True, timeout=60, allow_timeout=False):
            cmds.append(cmd)
            return subprocess.CompletedProcess(args=[], returncode=0,
                                                stdout='', stderr='')
        d.run_on = fake_run_on

        d.do_clean_logs()

        # 4 calls per node: TERM kill, -9 kill, rm run-time, test -x verify
        self.assertEqual(len(cmds), 4)
        self.assertIn('kill $p', cmds[0])         # Phase 1: graceful TERM
        self.assertNotIn('kill -9', cmds[0])
        self.assertIn('kill -9', cmds[1])        # Phase 2: force kill
        self.assertTrue(cmds[2].startswith('rm -rf '))  # Phase 3: rm output
        self.assertTrue(cmds[3].startswith('test -x '))  # Phase 4: verify

    @patch('deploy_client.time.sleep')
    def test_warns_when_binary_missing_after_clean(self, mock_sleep):
        # If install never ran (or a prior clean wiped the binary), the verify
        # `test -x` returns nonzero. The operator must see a WARNING so they
        # run install before the next start (instead of a confusing
        # "start FAILED: kvtest binary not found" on the next run).
        d = self._make_deployer()

        def fake_run_on(node, cmd, check=True, timeout=60, allow_timeout=False):
            if cmd.startswith('test -x '):
                return subprocess.CompletedProcess(args=[], returncode=1,
                                                    stdout='', stderr='')
            return subprocess.CompletedProcess(args=[], returncode=0,
                                                stdout='', stderr='')
        d.run_on = fake_run_on

        # Should not raise; the warning is logged, do_clean_logs still
        # returns normally (returns None, prints summary).
        with patch('deploy_client.log_info') as mock_log:
            d.do_clean_logs()
            msgs = ' '.join(str(c) for c in mock_log.call_args_list)
            self.assertIn('WARNING', msgs)
            self.assertIn('kvtest binary missing', msgs)

    @patch('deploy_client.time.sleep')
    def test_kills_both_kvtest_and_procmon(self, mock_sleep):
        # Both processes must be in the kill list: kvtest is the workload;
        # procmon.py is the watchdog that would otherwise restart kvtest
        # mid-clean and race the rm. Mirrors do_clean's kill pattern exactly.
        d = self._make_deployer()
        cmds = []

        def fake_run_on(node, cmd, check=True, timeout=60, allow_timeout=False):
            cmds.append(cmd)
            return subprocess.CompletedProcess(args=[], returncode=0,
                                                stdout='', stderr='')
        d.run_on = fake_run_on

        d.do_clean_logs()

        # Both kill phases must target both pgrep -x kvtest and pgrep -x procmon.py
        self.assertIn('pgrep -x kvtest', cmds[0])
        self.assertIn('pgrep -x procmon.py', cmds[0])
        self.assertIn('pgrep -x kvtest', cmds[1])
        self.assertIn('pgrep -x procmon.py', cmds[1])


class TestDoCollect(unittest.TestCase):
    """do_collect: single-phase pipeline (per-node summary -> collect, no
    global barrier), bounded pool, configurable summary-timeout, and
    node_slice for manual batching. Replaces the prior two-phase design
    where Phase 1 (summary) blocked on the slowest node for up to 60s
    before Phase 2 (collect) could start."""

    def _make_deployer(self, nodes=None):
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json',
                                         delete=False) as tf:
            json.dump({
                'nodes': nodes or [
                    {'host': '127.0.0.1', 'port': 9000, 'instance_id': '0'},
                    {'host': '127.0.0.2', 'port': 9000, 'instance_id': '1'},
                ],
                'remote_work_dir': '/tmp/kvtest',
            }, tf)
            deploy_json = tf.name
        try:
            d = Deployer.__new__(Deployer)
            d.deploy = json.load(open(deploy_json))
            d.config_template = {}
            d.base_dir = os.path.dirname(os.path.abspath(deploy_json))
            d.nodes = d.deploy.get('nodes', [])
            d.remote_work_dir = d.deploy.get('remote_work_dir', '')
            d.binary_path = None
            d.version = '?'
            d.default_transport = d.deploy.get('transport', 'ssh')
            d.default_ssh_user = d.deploy.get('ssh_user', 'root')
            d.ssh_options = d.deploy.get('ssh_options', '')
            d.enable_procmon = d.deploy.get('enable_procmon', False)
            d.listen_port = 9000
            d._host_locks = {}
            return d
        finally:
            os.unlink(deploy_json)

    @patch('deploy_client.time.sleep')
    @patch('deploy_client.os.path.isdir', return_value=False)
    def test_single_phase_summary_then_collect_per_node(self, mock_isdir, mock_sleep):
        # Each node must trigger its own /summary then immediately collect
        # its files in one thread -- no global barrier between summary and
        # collect. Verify the /summary POST is issued per node and collect
        # follows in the same thread.
        d = self._make_deployer()
        summary_cmds = []
        collect_calls = []

        def fake_run_on(node, cmd, check=True, timeout=60, allow_timeout=False):
            if '/summary' in cmd:
                summary_cmds.append((node['instance_id'], cmd))
                return subprocess.CompletedProcess(args=[], returncode=0,
                                                    stdout='', stderr='')
            # collect_files / collect_sdk_logs ls / cat commands.
            collect_calls.append((node['instance_id'], cmd))
            return subprocess.CompletedProcess(args=[], returncode=0,
                                                stdout='', stderr='')
        d.run_on = fake_run_on
        d.collect_files = lambda node, ld: collect_calls.append(
            (node['instance_id'], 'collect_files'))
        d.collect_sdk_logs = lambda node, ld, sd: collect_calls.append(
            (node['instance_id'], 'collect_sdk_logs'))

        with tempfile.TemporaryDirectory() as outdir:
            d.do_collect(output_dir=outdir, summary_timeout=5)

        # Both nodes must have triggered /summary.
        iids = {iid for iid, _ in summary_cmds}
        self.assertEqual(iids, {'0', '1'})
        # Both nodes must have called collect_files + collect_sdk_logs.
        collect_iids = {iid for iid, _ in collect_calls}
        self.assertEqual(collect_iids, {'0', '1'})

    @patch('deploy_client.time.sleep')
    @patch('deploy_client.os.path.isdir', return_value=False)
    def test_summary_timeout_caps_retries(self, mock_isdir, mock_sleep):
        # A node whose /summary always fails (rc!=0) must exhaust
        # summary_timeout then still collect. With summary_timeout=1 and
        # _POLL_INTERVAL=2, the while loop body runs once (deadline passes
        # before the second iteration's sleep). Verify the node still
        # gets collected.
        d = self._make_deployer(nodes=[
            {'host': '127.0.0.1', 'port': 9000, 'instance_id': '0'},
        ])
        summary_count = [0]

        def fake_run_on(node, cmd, check=True, timeout=60, allow_timeout=False):
            if '/summary' in cmd:
                summary_count[0] += 1
                return subprocess.CompletedProcess(args=[], returncode=1,
                                                    stdout='', stderr='')
            return subprocess.CompletedProcess(args=[], returncode=0,
                                                stdout='', stderr='')
        d.run_on = fake_run_on
        d.collect_files = lambda node, ld: None
        d.collect_sdk_logs = lambda node, ld, sd: None

        with tempfile.TemporaryDirectory() as outdir:
            d.do_collect(output_dir=outdir, summary_timeout=1)

        # /summary was retried at least once (timeout was honored).
        self.assertGreaterEqual(summary_count[0], 1)

    @patch('deploy_client.time.sleep')
    @patch('deploy_client.os.path.isdir', return_value=False)
    def test_node_slice_limits_nodes(self, mock_isdir, mock_sleep):
        # node_slice=(offset, count) must limit collect to a deterministic
        # slice. With 4 nodes and slice=(1, 2), only nodes[1] and nodes[2]
        # should be collected.
        nodes = [
            {'host': f'127.0.0.{i}', 'port': 9000, 'instance_id': str(i)}
            for i in range(4)
        ]
        d = self._make_deployer(nodes=nodes)
        collected_iids = []

        def fake_run_on(node, cmd, check=True, timeout=60, allow_timeout=False):
            if '/summary' in cmd:
                return subprocess.CompletedProcess(args=[], returncode=0,
                                                    stdout='', stderr='')
            return subprocess.CompletedProcess(args=[], returncode=0,
                                                stdout='', stderr='')
        d.run_on = fake_run_on
        d.collect_files = lambda node, ld: collected_iids.append(node['instance_id'])
        d.collect_sdk_logs = lambda node, ld, sd: None

        with tempfile.TemporaryDirectory() as outdir:
            d.do_collect(output_dir=outdir, summary_timeout=5,
                         node_slice=(1, 2))

        self.assertEqual(sorted(collected_iids), ['1', '2'])


if __name__ == '__main__':
    unittest.main()
