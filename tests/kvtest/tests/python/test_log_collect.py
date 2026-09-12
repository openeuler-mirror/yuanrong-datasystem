import importlib
import importlib.util
import io
import json
import gzip
import os
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))


class TestLogCollect(unittest.TestCase):
    def setUp(self):
        self.assertIsNotNone(importlib.util.find_spec('log_collect'), 'Shared collection filters are missing')
        self.collect = importlib.import_module('log_collect')

    def archive(self, root, **options):
        config = dict(sources=[['logs', str(root), ['*']]], patterns=[], keywords=[], uncompressed_only=False)
        config.update(options)
        result = subprocess.run([sys.executable, '-c', self.collect.REMOTE_ARCHIVE,
                                 json.dumps(config)], capture_output=True, check=True)
        with tarfile.open(fileobj=io.BytesIO(result.stdout), mode='r:gz') as archive:
            return {m.name: archive.extractfile(m).read() for m in archive if m.isfile()}

    def test_literal_keyword_and_file_filter(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / 'client access.log').write_bytes(b'other\nURMA_PERF a.*\nURMA_PERF abc\n')
            (root / 'worker.log').write_bytes(b'URMA_PERF a.*\n')
            files = self.archive(root, patterns=['*access*.log'], keywords=['a.*'])
            self.assertEqual(files, {'logs/client access.log.matched': b'URMA_PERF a.*\n'})

    def test_uncompressed_and_full_collection(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / 'a.log').write_bytes(b'latest\n')
            (root / 'a.log.gz').write_bytes(gzip.compress(b'old\n'))
            (root / 'a.log.1').write_bytes(b'rotation\n')
            self.assertEqual(len(self.archive(root)), 3)
            self.assertEqual(set(self.archive(root, uncompressed_only=True)), {'logs/a.log', 'logs/a.log.1'})

    def test_compressed_keywords_and_no_match(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / 'a.log.gz').write_bytes(gzip.compress(b'noise\nURMA_PERF yes\n'))
            self.assertEqual(self.archive(root, keywords=['URMA_PERF']),
                             {'logs/a.log.gz.matched': b'URMA_PERF yes\n'})
            self.assertEqual(self.archive(root, keywords=['absent']), {})

    def test_selection_is_exact_and_rejects_unknown(self):
        nodes = [{'name': 'pod-1'}, {'name': 'pod-10'}]
        self.assertEqual(self.collect.select_targets(nodes, ['pod-1'], 'name'), nodes[:1])
        with self.assertRaises(ValueError):
            self.collect.select_targets(nodes, ['missing'], 'name')

    def test_directory_contains_all_identities(self):
        self.assertEqual(self.collect.pod_directory('pod-1', '192.0.2.1', '192.0.2.2'),
                         'pod-1__podip-192.0.2.1__hostip-192.0.2.2')


    def test_receive_stream_and_reject_failed_transport(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / 'source'
            root.mkdir()
            (root / 'a.log').write_bytes(b'URMA_PERF yes\n')
            config = dict(sources=[['logs', str(root), ['*']]], patterns=[], keywords=[], uncompressed_only=False)
            output = Path(tmp) / 'out'
            count = self.collect.receive_archive([sys.executable, '-c', self.collect.REMOTE_ARCHIVE,
                                                  json.dumps(config)], output)
            self.assertEqual(count, 1)
            self.assertEqual((output / 'logs/a.log').read_bytes(), b'URMA_PERF yes\n')
            with self.assertRaises(RuntimeError):
                self.collect.receive_archive([sys.executable, '-c', 'raise SystemExit(2)'], output)

    def test_optional_sources_do_not_expand_to_unrelated_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / 'a.log').write_bytes(b'wanted\n')
            (root / 'secret.txt').write_bytes(b'excluded\n')
            config = [['logs', str(root), ['*.log'], False]]
            self.assertEqual(self.archive(root, sources=config, patterns=['*']), {'logs/a.log': b'wanted\n'})

    def test_worker_collect_uses_all_pods_selected_by_prefix(self):
        from types import SimpleNamespace
        from unittest.mock import patch
        import deploy_worker
        args = SimpleNamespace(file_pattern=['*access*.log'], keyword=['URMA_PERF'],
                               uncompressed_only=True, namespace='default', remote_config='/tmp/worker.config',
                               remote_dir='/tmp/worker', output='unused', timeout=10, max_workers=1, pod_info=True)
        pods = [dict(name=n, ip='192.0.2.1', host_ip='192.0.2.2') for n in ('pod-1', 'pod-10')]
        with patch('deploy_worker.read_remote_log_dir', return_value=('/logs', {})), \
             patch('deploy_worker.receive_archive', return_value=1) as receive, \
             patch('deploy_worker.collect_worker_config', return_value=True):
            self.assertEqual(deploy_worker.cmd_collect(args, pods), 0)
        command, directory, timeout = receive.call_args.args
        self.assertEqual({call.args[0][4] for call in receive.call_args_list}, {'pod-1', 'pod-10'})
        self.assertEqual(receive.call_count, 2)

    def test_client_selection_uses_live_pod_identity_and_skips_summary(self):
        from unittest.mock import Mock, patch
        from deploy_client import Deployer
        d = Deployer.__new__(Deployer)
        d.nodes = [dict(pod_name='pod-1', pod_ip='stale', instance_id='1'),
                   dict(pod_name='pod-10', instance_id='10')]
        d.default_transport = 'kubectl'
        d.remote_work_dir = '/tmp/client'
        d.listen_port = 9000
        d.run_on = Mock(side_effect=AssertionError('Filtered collection must not trigger summary'))
        options = dict(patterns=['*access*.log'], keywords=['URMA_PERF'], uncompressed_only=True)
        with tempfile.TemporaryDirectory() as tmp, \
             patch('deploy_client.get_pods', return_value=[dict(name='pod-1', ip='192.0.2.1', host_ip='192.0.2.2')]), \
             patch('deploy_client.receive_archive', return_value=1) as receive:
            self.assertEqual(d.do_collect(output_dir=tmp, filters=options, instance_ids=['1'], pod_info=True), 0)
        receive.assert_called_once()
        self.assertIn('pod-1__podip-192.0.2.1__hostip-192.0.2.2__client-1', receive.call_args.args[1])
        self.assertEqual(len(d.nodes), 2)
        d.run_on.assert_not_called()


    def test_default_worker_collect_delegates_without_new_options(self):
        from types import SimpleNamespace
        from unittest.mock import patch
        import deploy_worker
        args = SimpleNamespace(timeout=10, max_workers=None)
        pods = [dict(name='pod-1')]
        with patch('deploy_worker.cmd_collect_shared', return_value=0) as legacy, \
             patch('deploy_worker.receive_archive') as receive, \
             patch('deploy_worker.collect_worker_config', return_value=True) as config:
            self.assertEqual(deploy_worker.cmd_collect(args, pods), 0)
        legacy.assert_called_once_with(args, pods, 'worker logs', 10)
        config.assert_called_once_with(args, pods[0])
        receive.assert_not_called()
        self.assertIsNone(args.max_workers)

    def test_default_client_preserves_summary_and_directory_without_pod_lookup(self):
        from unittest.mock import Mock, patch
        from deploy_client import Deployer
        d = Deployer.__new__(Deployer)
        d.nodes = [dict(pod_name='pod-1', instance_id='1')]
        d.default_transport = 'kubectl'
        d.listen_port = 9000
        d.run_on = Mock(return_value=subprocess.CompletedProcess([], 0))
        d.collect_files = Mock()
        d.collect_sdk_logs = Mock()
        with tempfile.TemporaryDirectory() as tmp, patch('deploy_client.get_pods') as lookup, \
             patch('deploy_client.receive_archive') as receive:
            self.assertIsNone(d.do_collect(output_dir=tmp))
            d.collect_files.assert_called_once_with(d.nodes[0], os.path.join(tmp, 'pod-1_1'))
        self.assertIn('/summary', d.run_on.call_args.args[1])
        lookup.assert_not_called()
        receive.assert_not_called()

    def test_reject_archive_path_traversal(self):
        import base64
        payload = io.BytesIO()
        with tarfile.open(fileobj=payload, mode='w:gz') as archive:
            entry = tarfile.TarInfo('../escape')
            entry.size = 1
            archive.addfile(entry, io.BytesIO(b'x'))
        script = 'import base64,sys; sys.stdout.buffer.write(base64.b64decode(sys.argv[1]))'
        with tempfile.TemporaryDirectory() as tmp, self.assertRaises(ValueError):
            self.collect.receive_archive([sys.executable, '-c', script,
                                          base64.b64encode(payload.getvalue()).decode()], tmp)


    def test_only_input_configs_are_archived(self):
        with tempfile.TemporaryDirectory() as tmp:
            case = Path(tmp) / 'aaa'
            case.mkdir()
            (case / 'deploy.json').write_text('{}')
            (case / 'config.json').write_text('{"mode": "test"}')
            (case / 'data').mkdir()
            (case / 'data/input.txt').write_text('fixture')
            output = case / 'collected'
            output.mkdir()
            (output / 'old.log').write_text('exclude')
            self.assertTrue(hasattr(self.collect, 'copy_case_files'))
            self.collect.copy_case_files([case / 'deploy.json', case / 'config.json'], output)
            self.assertEqual((output / 'aaa/config.json').read_text(), '{"mode": "test"}')
            self.assertFalse((output / 'aaa/data').exists())
            self.assertFalse((output / 'aaa/collected').exists())

    def test_worker_config_is_collected_without_log_filtering(self):
        from types import SimpleNamespace
        from unittest.mock import patch
        import deploy_worker
        with tempfile.TemporaryDirectory() as tmp:
            args = SimpleNamespace(remote_config='/tmp/worker config.json', output=tmp,
                                   namespace='default', timeout=10, pod_info=False)
            pod = dict(name='worker-1', ip='192.0.2.1')
            self.assertTrue(hasattr(deploy_worker, 'collect_worker_config'))
            with patch('deploy_worker.kubectl_exec', return_value=subprocess.CompletedProcess([], 0, '{"port": 123}')):
                self.assertTrue(deploy_worker.collect_worker_config(args, pod))
            self.assertEqual(json.loads((Path(tmp) / 'worker-1/worker_config.json').read_text()), {'port': 123})


    def test_client_collect_archives_config_files_from_constructor_paths(self):
        from deploy_client import Deployer
        with tempfile.TemporaryDirectory() as tmp:
            case = Path(tmp) / 'aaa'
            case.mkdir()
            deploy = case / 'deploy.json'
            config = case / 'config.json'
            deploy.write_text('{"nodes": []}')
            config.write_text('{"listen_port": 9000}')
            output = Path(tmp) / 'logs'
            client = Deployer(str(deploy), str(config))
            client.do_collect(output_dir=str(output))
            self.assertEqual((output / 'aaa/deploy.json').read_bytes(), deploy.read_bytes())
            self.assertEqual((output / 'aaa/config.json').read_bytes(), config.read_bytes())


    def test_collect_clis_expose_prefix_instead_of_pods(self):
        root = Path(__file__).resolve().parents[2]
        for role in ('worker', 'client'):
            result = subprocess.run([sys.executable, str(root / ('deploy_' + role + '.py')), 'collect', '--help'],
                                    capture_output=True, text=True, check=True)
            self.assertNotIn('--pods', result.stdout)
            self.assertIn('--prefix', result.stdout)


    def test_actual_log_names_exclude_env_and_procmon_even_with_wildcard(self):
        logs = ['ds_client_3119.INFO.log', 'ds_client_3119_operation.log',
                'ds_client_access_3119.log', 'access.log', 'kvcache.INFO.log',
                'kvcache_operation.log', 'kv_metrics.log', 'kv_resource.log',
                'request_out.log', 'resource.log', 'resource_monitor.log']
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for name in logs + ['env', 'procmon.py']:
                (root / name).write_bytes(b'content\n')
            expected = {'logs/' + name for name in logs}
            self.assertEqual(set(self.archive(root, patterns=['*'])), expected)
            patterns = ['*access*.log', '*INFO*.log', '*operation*.log',
                        '*metrics*.log', '*request*.log', '*resource*.log']
            self.assertEqual(set(self.archive(root, patterns=patterns)), expected)


    def test_client_prefixes_match_any_prefix_without_duplicate_collection(self):
        from unittest.mock import patch
        from deploy_client import Deployer
        d = Deployer.__new__(Deployer)
        d.nodes = [dict(pod_name=name, instance_id=str(i))
                   for i, name in enumerate(['pod-1', 'pod-10', 'other-1'])]
        d.default_transport = 'kubectl'
        d.remote_work_dir = '/tmp/client'
        d.listen_port = 9000
        options = dict(patterns=['*.log'], keywords=[], uncompressed_only=False)
        with tempfile.TemporaryDirectory() as tmp, patch('deploy_client.receive_archive', return_value=1) as receive:
            self.assertEqual(d.do_collect(output_dir=tmp, filters=options, prefixes=['pod-', 'pod-1']), 0)
        self.assertEqual({c.args[0][2] for c in receive.call_args_list}, {'pod-1', 'pod-10'})
        self.assertEqual(receive.call_count, 2)
        self.assertEqual(len(d.nodes), 3)
        with patch('deploy_client.receive_archive') as receive:
            self.assertEqual(d.do_collect(filters=options, prefixes=['missing']), 1)
            receive.assert_not_called()


if __name__ == '__main__':
    unittest.main()
