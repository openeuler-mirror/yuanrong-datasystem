#!/usr/bin/env python3
"""Profiling configuration and deployment contract tests."""

import json
import os
import pathlib
import shlex
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2]))
from deploy_client import Deployer, main, normalize_jemalloc_prof_conf


class TestNormalizeJemallocProf(unittest.TestCase):
    def test_defaults_under_log_directory(self):
        conf, parent = normalize_jemalloc_prof_conf('lg_prof_sample:19', '/path/to/log', 7)
        self.assertEqual(parent, '/path/to/log/jemalloc')
        self.assertEqual(conf, 'lg_prof_sample:19,prof:true,prof_prefix:/path/to/log/jemalloc/kvtest_7')

    def test_explicit_prefix_and_sampling_policy(self):
        conf, parent = normalize_jemalloc_prof_conf(
            ' prof:TRUE, prof_active:TRUE,prof_final:true,prof_prefix:relative dir/custom ',
            '/ignored', 7)
        self.assertEqual(parent, 'relative dir')
        self.assertEqual(conf, 'prof:true,prof_active:true,prof_final:true,prof_prefix:relative dir/custom')

    def test_invalid_configs(self):
        for conf in ['', ' ', ',prof:true', 'prof:true,', 'prof', ':true', 'prof:',
                     'prof:false', 'prof:1', 'prof:true,prof:true', 'prof_prefix:heap',
                     'prof_prefix:x/\0heap', 'prof_prefix:x/\nheap', 'prof_active:false',
                     'prof_thread_active_init:false', 'prof_final:yes', 'lg_prof_sample:abc',
                     'lg_prof_sample:-1', 'lg_prof_sample:64', 'lg_prof_interval:-2',
                     'lg_prof_interval:64']:
            with self.subTest(conf=conf), self.assertRaises(ValueError):
                normalize_jemalloc_prof_conf(conf, '/logs', 0)

    def test_default_prefix_rejects_unrepresentable_log_directory(self):
        with self.assertRaises(ValueError):
            normalize_jemalloc_prof_conf('prof:true', '/logs,prof:false', 0)


class TestProfileDeployment(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.tmp.name)
        self.node = {'host': 'remote', 'host_ip': '192.0.2.1', 'instance_id': 3,
                     'remote_sdk_dir': '/sdk/lib'}
        deploy = self.root / 'deploy.json'
        config = self.root / 'config.json'
        deploy.write_text(json.dumps({'remote_work_dir': '/work', 'nodes': [self.node]}))
        config.write_text(json.dumps({'output_dir': '/path/to/log'}))
        self.d = Deployer(str(deploy), str(config))
        self.d.base_dir = str(self.root)
        self.d.binary_path = str(self.root / 'kvtest')
        pathlib.Path(self.d.binary_path).write_text('binary')
        (self.root / 'lib').mkdir()
        (self.root / 'lib/libjemalloc.so.2').write_text('runtime')
        self.d.jemalloc_prof_conf = 'lg_prof_sample:19,prof_final:true'
        self.commands = []
        self.uploads = []
        self.configs = []
        self.supported = True
        self.directory_error = False

    def tearDown(self):
        self.tmp.cleanup()

    def run_on(self, node, command, **kwargs):
        self.commands.append(command)
        if command == 'pgrep -x kvtest':
            return subprocess.CompletedProcess(command, 1, stdout='', stderr='')
        if command == 'test -f /work/standalone_launcher.py':
            exists = (self.root / 'standalone_launcher.py').exists()
            return subprocess.CompletedProcess(command, 0 if exists else 1, stdout='', stderr='')
        if '--version' in command:
            output = f'jemalloc_prof_supported={str(self.supported).lower()}\n'
        elif 'test -w' in command:
            if self.directory_error:
                raise RuntimeError('profile directory not writable')
            output = ''
        else:
            output = '123\n'
        return subprocess.CompletedProcess(command, 0, stdout=output, stderr='')

    def scp_to(self, node, src, dst):
        self.uploads.append((src, dst))
        if dst.endswith('config_3.json'):
            with open(src) as file:
                self.configs.append(json.load(file))

    def deploy(self, launcher):
        if launcher:
            (self.root / 'standalone_launcher.py').write_text('launcher')
        with patch('deploy_client.__file__', str(self.root / 'deploy_client.py')), \
                patch('deploy_client.time.sleep'), \
                patch.object(self.d, 'run_on', side_effect=self.run_on), \
                patch.object(self.d, 'scp_to', side_effect=self.scp_to):
            return self.d.deploy_node(self.node)[0]

    def test_both_launch_paths_receive_conf_and_bundled_runtime(self):
        for launcher in [False, True]:
            with self.subTest(launcher=launcher):
                self.commands.clear()
                self.assertTrue(self.deploy(launcher))
                launch = next(cmd for cmd in self.commands
                              if '--binary' in cmd or 'nohup ./kvtest' in cmd)
                self.assertIn('prof_prefix:/path/to/log/jemalloc/kvtest_3', launch)
                self.assertIn('MALLOC_CONF=', launch)
                self.assertIn('/work/allocator_lib:/sdk/lib', launch)
                self.assertNotIn('/work/lib:', launch)
                self.assertTrue(any(dst == '/work/allocator_lib/libjemalloc.so.2' for _, dst in self.uploads))
                prepare = next(cmd for cmd in self.commands if 'test -w' in cmd)
                self.assertIn('/path/to/log/jemalloc', prepare)
                self.assertLess(self.commands.index(prepare), self.commands.index(launch))
                self.assertEqual(self.configs[-1]['output_dir'], '/path/to/log')

    def test_missing_output_dir_is_fixed_in_instance_config(self):
        self.d.config_template = {}
        self.assertTrue(self.deploy(False))
        output_dir = self.configs[-1]['output_dir']
        self.assertTrue(output_dir.startswith('metrics_3_'))
        self.assertTrue(any(f'prof_prefix:{output_dir}/jemalloc/kvtest_3' in cmd
                            for cmd in self.commands))

    def test_custom_library_path_does_not_override_bundled_runtime(self):
        self.d.config_template['env'] = {'LD_LIBRARY_PATH': '/custom/lib'}
        self.assertTrue(self.deploy(False))
        commands = [cmd for cmd in self.commands if '--version' in cmd or 'nohup ./kvtest' in cmd]
        self.assertEqual(len(commands), 2)
        for command in commands:
            self.assertIn('LD_LIBRARY_PATH=/work/allocator_lib:/sdk/lib:/custom/lib', command)

    def test_bundle_upload_without_sdk_uses_flat_lib_directory(self):
        self.node.pop('remote_sdk_dir')
        (self.root / 'lib/liburma.so').write_text('urma')
        self.assertTrue(self.deploy(False))
        targets = [dst for _, dst in self.uploads]
        self.assertEqual(targets.count('/work/allocator_lib/libjemalloc.so.2'), 1)
        self.assertEqual(targets.count('/work/lib/liburma.so'), 1)
        self.assertNotIn('/work/lib', targets)

    def test_no_option_keeps_existing_output_dir_behavior(self):
        self.d.jemalloc_prof_conf = None
        self.d.config_template = {}
        self.assertTrue(self.deploy(False))
        self.assertNotIn('output_dir', self.configs[-1])
        self.assertFalse(any('MALLOC_CONF' in cmd or '--version' in cmd for cmd in self.commands))
        self.assertTrue(any(dst == '/work/allocator_lib/libjemalloc.so.2' for _, dst in self.uploads))

    def test_unsupported_or_unwritable_prevents_launch(self):
        for unsupported in [False, True]:
            with self.subTest(unsupported=unsupported):
                self.supported = not unsupported
                self.directory_error = not unsupported
                self.commands.clear()
                self.assertFalse(self.deploy(False))
                self.assertFalse(any('nohup ./kvtest' in cmd for cmd in self.commands))

    def test_relative_quoted_path_is_prepared_on_target(self):
        self.d.jemalloc_prof_conf = "prof_prefix:logs with ' quotes/custom"
        env = {'MALLOC_CONF': 'prof:false'}
        with patch.object(self.d, 'run_on', side_effect=self.run_on):
            self.d.prepare_jemalloc_prof_env(self.node, {'output_dir': '/ignored'}, '/lib', env)
        directory_cmd = self.commands[-1]
        self.assertIn('cd /work &&', directory_cmd)
        self.assertIn(shlex.quote("logs with ' quotes"), directory_cmd)
        self.assertIn("MALLOC_CONF=''", self.commands[0])
        self.assertIn('prof:true', env['MALLOC_CONF'])

    def test_remote_directory_command_uses_target_cwd(self):
        self.d.remote_work_dir = str(self.root)
        binary = pathlib.Path(self.d.binary_path)
        binary.write_text('#!/bin/sh\necho jemalloc_prof_supported=true\n')
        binary.chmod(0o755)
        self.d.jemalloc_prof_conf = "prof_prefix:logs with ' quotes/custom"
        env = {}

        def execute(node, command, **kwargs):
            return subprocess.run(command, shell=True, text=True, capture_output=True,
                                  check=kwargs.get('check', True))

        with patch.object(self.d, 'run_on', side_effect=execute):
            self.d.prepare_jemalloc_prof_env(self.node, {'output_dir': '/ignored'}, '', env)
        self.assertTrue((self.root / "logs with ' quotes").is_dir())
        self.assertIn('prof:true', env['MALLOC_CONF'])

    def test_cli_rejects_invalid_conf_before_deploy(self):
        argv = ['deploy_client.py', 'deploy', str(self.root / 'deploy.json'),
                str(self.root / 'config.json'), '--jemalloc_prof_conf', 'prof:false']
        with patch.object(sys, 'argv', argv), patch.object(Deployer, 'do_deploy') as deploy:
            with self.assertRaises(SystemExit) as raised:
                main()
            self.assertEqual(raised.exception.code, 2)
            deploy.assert_not_called()

    def test_cli_deploy_failure_does_not_exit(self):
        argv = ['deploy_client.py', 'deploy', str(self.root / 'deploy.json'),
                str(self.root / 'config.json'), '--jemalloc_prof_conf', 'prof:true']
        with patch.object(sys, 'argv', argv), patch.object(Deployer, 'do_deploy', return_value=False):
            main()

    def test_start_cli_passes_profile_conf(self):
        argv = ['deploy_client.py', 'start', str(self.root / 'deploy.json'),
                str(self.root / 'config.json'), '--jemalloc_prof_conf', 'prof:true']
        with patch.object(sys, 'argv', argv), \
                patch.object(Deployer, 'do_start', autospec=True, return_value=True) as start:
            main()
            self.assertEqual(start.call_args[0][0].jemalloc_prof_conf, 'prof:true')

    def test_install_failure_still_starts(self):
        with patch.object(self.d, 'do_install', return_value=False), \
                patch.object(self.d, 'do_start', return_value=True) as start:
            self.assertTrue(self.d.do_deploy())
            start.assert_called_once_with(keep_remote_config=False)

    def test_deploy_propagates_start_result(self):
        with patch.object(self.d, 'do_install', return_value=True), \
                patch.object(self.d, 'do_start', return_value=False) as start:
            self.assertFalse(self.d.do_deploy())
            start.assert_called_once_with(keep_remote_config=False)

    def test_start_only_does_not_upload_runtime(self):
        with patch.object(self.d, 'run_on', side_effect=self.run_on), \
                patch.object(self.d, 'scp_to', side_effect=self.scp_to):
            self.assertTrue(self.d.start_node(self.node)[0])
        self.assertEqual([dst for _, dst in self.uploads], ['/work/config_3.json'])
        self.assertTrue(any('MALLOC_CONF=' in cmd for cmd in self.commands))

    def test_running_client_rejects_profile_but_preserves_plain_start(self):
        for conf, expected in [('prof:true', False), (None, True)]:
            self.d.jemalloc_prof_conf = conf
            with self.subTest(conf=conf), \
                    patch.object(self.d, 'run_on', return_value=subprocess.CompletedProcess(
                        [], 0, stdout='123\n', stderr='')), \
                    patch.object(self.d, 'scp_to'), \
                    patch.object(self.d, 'prepare_jemalloc_prof_env') as prepare:
                self.assertEqual(self.d.start_node(self.node)[0], expected)
                prepare.assert_not_called()

    def test_disabled_sampling_cli_rejected_before_deployment(self):
        for action in ['start', 'deploy']:
            argv = ['deploy_client.py', action, str(self.root / 'deploy.json'),
                    str(self.root / 'config.json'), '--jemalloc_prof_conf', 'prof_active:false']
            with patch.object(sys, 'argv', argv), \
                    patch.object(Deployer, 'do_' + action) as operation:
                with self.assertRaises(SystemExit) as error:
                    main()
                self.assertEqual(error.exception.code, 2)
                operation.assert_not_called()

    def test_packaged_cli_resolves_adjacent_binary(self):
        argv = ['deploy_client.py', 'deploy', str(self.root / 'deploy.json'),
                str(self.root / 'config.json')]
        with patch.object(sys, 'argv', argv), \
                patch('deploy_client.__file__', str(self.root / 'deploy_client.py')), \
                patch.object(Deployer, 'do_deploy', autospec=True, return_value=True) as deploy:
            main()
            self.assertEqual(deploy.call_args[0][0].binary_path, self.d.binary_path)


if __name__ == '__main__':
    unittest.main()
