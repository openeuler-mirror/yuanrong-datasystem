import json
import shlex
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import deploy_worker


class TestWorkerCollectLogDir(unittest.TestCase):
    def test_cli_accepts_override_and_defaults_to_none(self):
        import subprocess
        command = [sys.executable, str(Path(deploy_worker.__file__)), 'collect', '--help']
        result = subprocess.run(command, capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('--log-dir', result.stdout)
        for arguments, expected in (([], None), (['--log-dir', '/custom/logs'], '/custom/logs')):
            with patch.object(sys, 'argv', ['deploy_worker.py', 'collect', '-p', 'worker-'] + arguments), \
                 patch('deploy_worker.get_pods', return_value=[dict(name='worker-1', ip='192.0.2.1')]), \
                 patch('deploy_worker.cmd_collect', return_value=0) as collect:
                deploy_worker.main()
            self.assertEqual(collect.call_args.args[0].log_dir, expected)

    def test_override_and_config_fallback_for_both_collection_paths(self):
        pods = [dict(name='worker-1', ip='192.0.2.1'), dict(name='worker-2', ip='192.0.2.2')]
        for filtered in (False, True):
            for override in (None, '/custom worker/logs'):
                with self.subTest(filtered=filtered, override=override), tempfile.TemporaryDirectory() as output:
                    args = SimpleNamespace(namespace='default', remote_config='/tmp/worker.config',
                                           remote_dir=None, output=output, timeout=10, max_workers=1,
                                           log_dir=override, file_pattern=['*.log'] if filtered else [],
                                           keyword=[], uncompressed_only=False)
                    with patch('deploy_worker.collect_worker_config', return_value=True) as config, \
                         patch('deploy_worker.read_remote_log_dir', return_value=('/config/logs', {})) as filtered_read, \
                         patch('deploy_common.read_remote_log_dir', return_value=('/config/logs', {})) as full_read, \
                         patch('deploy_common._collect_stagger_delay', return_value=0), \
                         patch('deploy_common.collect_logs_from_pod', return_value=True) as collect, \
                         patch('deploy_worker.receive_archive', return_value=1) as receive:
                        self.assertEqual(deploy_worker.cmd_collect(args, pods), 0)
                    self.assertEqual(config.call_count, 2)
                    self.assertEqual(filtered_read.call_count, 2 if filtered and override is None else 0)
                    self.assertEqual(full_read.call_count, 1 if not filtered and override is None else 0)
                    expected = override or '/config/logs'
                    if filtered:
                        self.assertEqual(receive.call_count, 2)
                        for call in receive.call_args_list:
                            cfg = json.loads(shlex.split(call.args[0][-1])[-1])
                            self.assertEqual(cfg['sources'][0][1], expected)
                    else:
                        self.assertEqual(collect.call_count, 2)
                        self.assertTrue(all(call.args[2] == expected for call in collect.call_args_list))


if __name__ == '__main__':
    unittest.main()
