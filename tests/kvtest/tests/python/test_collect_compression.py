import argparse
import json
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import log_collect


class TestCollectCompression(unittest.TestCase):
    def test_cli_uses_defaults_and_rejects_removed_flags(self):
        import contextlib
        import io
        parser = argparse.ArgumentParser()
        log_collect.add_collect_filters(parser)
        args = parser.parse_args([])
        self.assertFalse(hasattr(args, 'compress'))
        self.assertFalse(hasattr(args, 'extract'))
        self.assertFalse(args.uncompressed_only)
        self.assertTrue(parser.parse_args(['--uncompressed-only']).uncompressed_only)
        for flag in ('--compress', '--no-compress', '--extract', '--no-extract'):
            with self.subTest(flag=flag), contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit) as error:
                    parser.parse_args([flag])
                self.assertEqual(error.exception.code, 2)

    def test_filtered_archives_compressed_or_raw_extracted_or_retained(self):
        for compress in (True, False):
            for extract in (True, False):
                with self.subTest(compress=compress, extract=extract), tempfile.TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    source = root / 'source'
                    source.mkdir()
                    (source / 'access.log').write_bytes(b'noise\nURMA_PERF selected\n')
                    cfg = dict(sources=[['logs', str(source), ['*.log']]], patterns=[],
                               keywords=['URMA_PERF'], uncompressed_only=False, compress=compress)
                    command = [sys.executable, '-c', log_collect.REMOTE_ARCHIVE, json.dumps(cfg)]
                    options = dict(compress=compress, extract=extract)
                    self.assertEqual(log_collect.receive_archive(command, root / 'out', archive_options=options), 1)
                    output = root / 'out/logs/access.log.matched'
                    saved = root / ('out/logs.tar.gz' if compress else 'out/logs.tar')
                    if extract:
                        self.assertEqual(output.read_bytes(), b'URMA_PERF selected\n')
                        self.assertFalse(saved.exists())
                    else:
                        self.assertFalse(output.exists())
                        self.assertEqual(saved.read_bytes()[:2] == b'\x1f\x8b', compress)
                        with tarfile.open(saved, 'r:*') as archive:
                            self.assertEqual(archive.extractfile('logs/access.log.matched').read(),
                                             b'URMA_PERF selected\n')

    def test_failed_transfer_does_not_replace_existing_archive(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / 'logs.tar.gz'
            target.write_bytes(b'existing archive')
            with self.assertRaises(RuntimeError):
                log_collect.receive_archive([sys.executable, '-c', 'raise SystemExit(1)'], tmp,
                                            archive_options=dict(compress=True, extract=False))
            self.assertEqual(target.read_bytes(), b'existing archive')
            self.assertEqual(list(Path(tmp).iterdir()), [target])


    def test_full_worker_uses_requested_transfer_and_never_falls_back_on_failure(self):
        from unittest.mock import Mock, patch
        from deploy_common import collect_logs_from_pod
        pod = dict(name='worker-1', ip='192.0.2.1')
        options = dict(compress=False, extract=False)
        def remote(name, namespace, command, **kwargs):
            self.assertNotIn('base64', command)
            return subprocess.CompletedProcess([], 0, '/logs/stdout.log\n')
        with tempfile.TemporaryDirectory() as tmp, \
             patch('deploy_common.kubectl_exec', side_effect=remote), \
             patch('log_collect.receive_archive', return_value=1) as receive:
            self.assertTrue(collect_logs_from_pod(pod, 'default', '/logs', tmp, remote_dir='/logs', archive_options=options))
            self.assertIn('tar cf -', receive.call_args.args[0][-1])
            self.assertEqual(receive.call_args.args[0][-1].count('/logs/stdout.log'), 1)
            self.assertEqual(receive.call_args.kwargs['archive_options'], options)
            receive.side_effect = RuntimeError('failed transfer')
            self.assertFalse(collect_logs_from_pod(pod, 'default', '/logs', tmp, remote_dir='/logs', archive_options=options))

    def test_full_client_uses_distinct_archives_for_output_and_sdk_on_all_transports(self):
        from unittest.mock import patch
        from deploy_client import Deployer
        d = Deployer.__new__(Deployer)
        d.default_ssh_user = 'test'
        d.ssh_options = ''
        options = dict(compress=True, extract=False)
        for transport in ('kubectl', 'ssh', 'localhost'):
            d.default_transport = transport
            node = dict(pod_name='client-1', host='localhost' if transport == 'localhost' else '192.0.2.1', instance_id='1')
            with tempfile.TemporaryDirectory() as tmp, patch('deploy_client.receive_archive', return_value=1) as receive:
                for label in ('output files', 'SDK log files'):
                    d._collect_remote_files(node, tmp, ['/logs/a.log'], file_label=label,
                                            remote_dir='/logs', archive_options=options)
                self.assertEqual([c.kwargs['archive_name'] for c in receive.call_args_list], ['output', 'sdk'])
                for call in receive.call_args_list:
                    self.assertEqual(call.kwargs['archive_options'], options)
                    self.assertEqual(call.kwargs['shell'], transport == 'localhost')
                    command = call.args[0]
                    self.assertIn('tar czf -', command if isinstance(command, str) else command[-1])

    def test_filtered_worker_and_client_forward_archive_options(self):
        from types import SimpleNamespace
        from unittest.mock import patch
        import deploy_worker
        from deploy_client import Deployer
        options = dict(compress=False, extract=False)
        with tempfile.TemporaryDirectory() as tmp:
            args = SimpleNamespace(file_pattern=['*.log'], keyword=[], uncompressed_only=False,
                                   namespace='default', remote_config='/tmp/worker.json', remote_dir=None,
                                   output=tmp, timeout=10, max_workers=1, compress=False, extract=False)
            with patch('deploy_worker.collect_worker_config', return_value=True), \
                 patch('deploy_worker.read_remote_log_dir', return_value=('/logs', {})), \
                 patch('deploy_worker.receive_archive', return_value=1) as receive:
                self.assertEqual(deploy_worker.cmd_collect(args, [dict(name='worker-1', ip='192.0.2.1')]), 0)
                self.assertEqual(receive.call_args.kwargs['archive_options'], options)
            d = Deployer.__new__(Deployer)
            d.default_transport = 'kubectl'
            d.nodes = [dict(pod_name='client-1', instance_id='1')]
            d.remote_work_dir = '/tmp/client'
            d.listen_port = 9000
            filters = dict(patterns=['*.log'], keywords=[], uncompressed_only=False)
            with patch('deploy_client.receive_archive', return_value=1) as receive:
                self.assertEqual(d.do_collect(output_dir=tmp, filters=filters, archive_options=options), 0)
                self.assertEqual(receive.call_args.kwargs['archive_options'], options)


    def test_gnu_tar_hardlinks_are_materialized_or_retained_safely(self):
        import base64
        import io
        payload = io.BytesIO()
        with tarfile.open(fileobj=payload, mode='w:gz') as archive:
            original = tarfile.TarInfo('stdout.log')
            original.size = 3
            archive.addfile(original, io.BytesIO(b'log'))
            link = tarfile.TarInfo('alias.log')
            link.type = tarfile.LNKTYPE
            link.linkname = 'stdout.log'
            archive.addfile(link)
        command = [sys.executable, '-c',
                   'import sys,base64; sys.stdout.buffer.write(base64.b64decode(sys.argv[1]))',
                   base64.b64encode(payload.getvalue()).decode()]
        for extract in (True, False):
            with tempfile.TemporaryDirectory() as tmp:
                self.assertEqual(log_collect.receive_archive(command, tmp,
                                 archive_options=dict(compress=True, extract=extract)), 2)
                if extract:
                    self.assertEqual((Path(tmp) / 'alias.log').read_bytes(), b'log')
                    self.assertFalse((Path(tmp) / 'alias.log').is_symlink())
                else:
                    self.assertTrue((Path(tmp) / 'logs.tar.gz').exists())


if __name__ == '__main__':
    unittest.main()
