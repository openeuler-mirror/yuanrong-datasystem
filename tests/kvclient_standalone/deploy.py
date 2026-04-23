#!/usr/bin/env python3
"""Deploy kvclient_standalone_test to remote nodes."""

import json
import os
import shutil
import subprocess
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path


class Deployer:
    def __init__(self, deploy_path, config_template_path):
        with open(deploy_path) as f:
            self.deploy = json.load(f)
        with open(config_template_path) as f:
            self.config_template = json.load(f)

        self.nodes = self.deploy.get('nodes', [])
        self.remote_work_dir = self.deploy.get('remote_work_dir', '')
        self.binary_path = self.deploy.get('binary_path', '')
        self.default_ssh_user = self.deploy.get('ssh_user', 'root')
        self.ssh_options = self.deploy.get('ssh_options', '-o StrictHostKeyChecking=no')
        self.sdk_lib_dir = self.deploy.get('sdk_lib_dir', '')
        self.listen_port = self.config_template.get('listen_port', 9000)

    def _ssh_args(self):
        return self.ssh_options.split() if self.ssh_options else []

    def _user_for(self, node):
        return node.get('ssh_user', self.default_ssh_user)

    def run_on(self, host, user, cmd, check=True):
        if host == 'localhost':
            return subprocess.run(cmd, shell=True, check=check, capture_output=True, text=True)
        else:
            full_cmd = ['ssh'] + self._ssh_args() + [f'{user}@{host}', cmd]
            return subprocess.run(full_cmd, check=check, capture_output=True, text=True)

    def scp_to(self, host, user, src, dst):
        if host == 'localhost':
            if os.path.isdir(src):
                if os.path.exists(dst):
                    shutil.rmtree(dst)
                shutil.copytree(src, dst)
            else:
                os.makedirs(os.path.dirname(dst), exist_ok=True)
                shutil.copy2(src, dst)
        else:
            subprocess.run(
                ['scp'] + self._ssh_args() + ['-r', src, f'{user}@{host}:{dst}'],
                check=True
            )

    def build_default_peers(self):
        port = self.listen_port
        return [f'http://{n["host"]}:{port}' for n in self.nodes]

    def build_peers(self, node):
        if 'peers' in node:
            return node['peers']
        return self.build_default_peers()

    def build_node_overrides(self, node):
        override_keys = ('role', 'pipeline', 'notify_pipeline')
        return {k: v for k, v in node.items() if k in override_keys}

    def generate_config(self, node):
        config = dict(self.config_template)
        config['instance_id'] = node['instance_id']
        config['peers'] = self.build_peers(node)
        config.update(self.build_node_overrides(node))
        return config

    def deploy_node(self, node):
        host = node['host']
        instance_id = node['instance_id']
        user = self._user_for(node)
        is_local = host == 'localhost'

        print(f'Deploying to {host} (instance_id={instance_id}, local={is_local})...')

        config = self.generate_config(node)

        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.json', prefix=f'config_{instance_id}_',
            delete=False
        ) as tf:
            json.dump(config, tf, indent=2)
            tmp_config = tf.name

        try:
            self.run_on(host, user, f'mkdir -p {self.remote_work_dir}')

            remote_binary = f'{self.remote_work_dir}/kvclient_standalone_test'
            self.scp_to(host, user, self.binary_path, remote_binary)

            if self.sdk_lib_dir and os.path.isdir(self.sdk_lib_dir):
                print(f'  Deploying SDK libs to {host}...')
                self.run_on(host, user, f'rm -rf {self.remote_work_dir}/sdk_lib')
                self.scp_to(host, user, self.sdk_lib_dir, f'{self.remote_work_dir}/sdk_lib')

            remote_config = f'{self.remote_work_dir}/config_{instance_id}.json'
            self.scp_to(host, user, tmp_config, remote_config)

            self.run_on(host, user, f'chmod +x {self.remote_work_dir}/kvclient_standalone_test')

            ld_path = f'{self.remote_work_dir}/sdk_lib' if self.sdk_lib_dir else ''
            env_prefix = f'LD_LIBRARY_PATH={ld_path}:$LD_LIBRARY_PATH ' if ld_path else ''
            start_cmd = (
                f'cd {self.remote_work_dir} && '
                f'{env_prefix}'
                f'nohup ./kvclient_standalone_test config_{instance_id}.json '
                f'> stdout_{instance_id}.log 2>&1 &'
            )
            self.run_on(host, user, start_cmd)

            print(f'  {host} -> OK')
            return True
        except Exception as e:
            print(f'  {host} -> FAILED: {e}')
            return False
        finally:
            os.unlink(tmp_config)

    def do_deploy(self):
        results = []
        with ThreadPoolExecutor(max_workers=len(self.nodes) or 1) as pool:
            futures = {pool.submit(self.deploy_node, node): node for node in self.nodes}
            for future in as_completed(futures):
                results.append(future.result())

        ok = sum(1 for r in results if r)
        total = len(results)
        print(f'\nDeploy result: {ok}/{total} succeeded')

    def do_stop(self):
        config = dict(self.config_template)
        config['peers'] = self.build_default_peers()

        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.json', prefix='config_full_', delete=False
        ) as tf:
            json.dump(config, tf, indent=2)
            tmp_config = tf.name

        try:
            ld_path = ''
            if self.sdk_lib_dir:
                sdk_dir = os.path.abspath(self.sdk_lib_dir) if os.path.isdir(self.sdk_lib_dir) else self.sdk_lib_dir
                ld_path = sdk_dir

            print('Stopping all instances...')
            env = os.environ.copy()
            if ld_path:
                env['LD_LIBRARY_PATH'] = f'{ld_path}:{env.get("LD_LIBRARY_PATH", "")}'
            subprocess.run(
                [self.binary_path, '--stop', tmp_config],
                env=env, check=True
            )
        finally:
            os.unlink(tmp_config)

    def do_clean(self):
        results = []
        def clean_node(node):
            host = node['host']
            user = self._user_for(node)
            print(f'Cleaning {host}...')
            try:
                self.run_on(
                    host, user,
                    f'pkill -f kvclient_standalone_test; rm -rf {self.remote_work_dir}',
                    check=False
                )
                print(f'  {host} -> OK')
                return True
            except Exception as e:
                print(f'  {host} -> FAILED ({e})')
                return False

        with ThreadPoolExecutor(max_workers=len(self.nodes) or 1) as pool:
            futures = [pool.submit(clean_node, node) for node in self.nodes]
            for future in as_completed(futures):
                results.append(future.result())

        ok = sum(1 for r in results if r)
        print(f'\nClean result: {ok}/{len(results)}')


def main():
    if len(sys.argv) < 3:
        print('Usage: deploy.py --deploy|--stop|--clean <deploy.json> [config_template.json]')
        print()
        print('  --deploy  Deploy binary + SDK libs + config to all nodes and start')
        print('  --stop    Stop all instances via kvclient_standalone_test --stop')
        print('  --clean   Kill processes and remove remote work dirs')
        sys.exit(1)

    action = sys.argv[1]
    deploy_json = sys.argv[2]
    config_template = sys.argv[3] if len(sys.argv) > 3 else 'config.json.example'

    if action not in ('--deploy', '--stop', '--clean'):
        print(f'Unknown action: {action}')
        sys.exit(1)

    deployer = Deployer(deploy_json, config_template)

    if action == '--deploy':
        deployer.do_deploy()
    elif action == '--stop':
        deployer.do_stop()
    elif action == '--clean':
        deployer.do_clean()


if __name__ == '__main__':
    main()
