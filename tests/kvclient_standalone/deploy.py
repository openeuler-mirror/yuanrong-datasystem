#!/usr/bin/env python3
"""Deploy kvclient_standalone_test to remote nodes via SSH or kubectl."""

import json
import os
import shutil
import subprocess
import sys
import tarfile
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed


class Deployer:
    def __init__(self, deploy_path, config_template_path):
        with open(deploy_path) as f:
            self.deploy = json.load(f)
        with open(config_template_path) as f:
            self.config_template = json.load(f)

        self.base_dir = os.path.dirname(os.path.abspath(deploy_path))
        self.nodes = self.deploy.get('nodes', [])
        self.remote_work_dir = self.deploy.get('remote_work_dir', '')
        self.binary_path = os.path.join(self.base_dir, 'output', 'kvclient_standalone_test')
        self.datasystem_sdk_dir = os.path.join(self.base_dir, 'output', 'lib')
        self.default_transport = self.deploy.get('transport', 'ssh')
        self.default_ssh_user = self.deploy.get('ssh_user', 'root')
        self.ssh_options = self.deploy.get('ssh_options', '-o StrictHostKeyChecking=no')
        self.enable_procmon = self.deploy.get('enable_procmon', True)
        self.listen_port = self.config_template.get('listen_port', 9000)

    # --- Transport helpers ---

    def _ssh_args(self):
        return self.ssh_options.split() if self.ssh_options else []

    def _user_for(self, node):
        return node.get('ssh_user', self.default_ssh_user)

    def _transport(self, node):
        if node.get('host') == 'localhost':
            return 'localhost'
        return node.get('transport', self.default_transport)

    def _exec_target(self, node):
        """Target for run_on / scp_to (IP for SSH, pod name for kubectl)."""
        if self._transport(node) == 'kubectl':
            return node.get('pod_name', '')
        return node.get('host', 'localhost')

    def _namespace(self, node):
        return node.get('namespace', 'default')

    def _comm_host(self, node):
        """Network-reachable host for kvclient inter-instance communication."""
        if self._transport(node) == 'kubectl':
            return node.get('pod_ip', node.get('pod_name', ''))
        return node.get('host', 'localhost')

    # --- Transport primitives ---

    def run_on(self, node, cmd, check=True):
        """Run command on node via SSH, kubectl exec, or local shell."""
        transport = self._transport(node)
        target = self._exec_target(node)

        if transport == 'localhost':
            return subprocess.run(cmd, shell=True, check=check,
                                  capture_output=True, text=True)
        elif transport == 'kubectl':
            ns = self._namespace(node)
            return subprocess.run(
                ['kubectl', 'exec', target, '-n', ns, '--', 'sh', '-c', cmd],
                check=check, capture_output=True, text=True)
        else:
            user = self._user_for(node)
            return subprocess.run(
                ['ssh'] + self._ssh_args() + [f'{user}@{target}', cmd],
                check=check, capture_output=True, text=True)

    def scp_to(self, node, src, dst):
        """Copy file or directory to node via SCP, kubectl cp, or local copy."""
        transport = self._transport(node)
        target = self._exec_target(node)

        if transport == 'localhost':
            if os.path.isdir(src):
                if os.path.exists(dst):
                    shutil.rmtree(dst)
                shutil.copytree(src, dst)
            else:
                os.makedirs(os.path.dirname(dst), exist_ok=True)
                shutil.copy2(src, dst)
        elif transport == 'kubectl':
            ns = self._namespace(node)
            if os.path.isdir(src):
                # kubectl cp does not handle directories well; use tar
                tar_file = tempfile.NamedTemporaryFile(suffix='.tar.gz', delete=False)
                tar_path = tar_file.name
                tar_file.close()
                try:
                    with tarfile.open(tar_path, 'w:gz') as tar:
                        tar.add(src, arcname=os.path.basename(src))
                    remote_tar = f'{dst}.tar.gz'
                    subprocess.run(
                        ['kubectl', 'cp', tar_path, f'{ns}/{target}:{remote_tar}'],
                        check=True)
                    self.run_on(
                        node,
                        f'mkdir -p {dst} && tar xzf {remote_tar} -C {os.path.dirname(dst)} && rm -f {remote_tar}')
                finally:
                    if os.path.exists(tar_path):
                        os.unlink(tar_path)
            else:
                subprocess.run(
                    ['kubectl', 'cp', src, f'{ns}/{target}:{dst}'],
                    check=True)
        else:
            user = self._user_for(node)
            subprocess.run(
                ['scp'] + self._ssh_args() + ['-r', src, f'{user}@{target}:{dst}'],
                check=True)

    def collect_files(self, node, local_dir):
        """Collect output files from node via tar."""
        iid = node['instance_id']
        tar_remote = f'/tmp/collect_{iid}.tar.gz'
        tar_local = f'/tmp/collect_{iid}.tar.gz'
        transport = self._transport(node)
        target = self._exec_target(node)

        os.makedirs(local_dir, exist_ok=True)

        # Pack on remote
        self.run_on(
            node,
            f'cd {self.remote_work_dir} && tar czf {tar_remote} *.csv *.txt *.log 2>/dev/null || true',
            check=False)

        # Pull to local
        try:
            if transport == 'localhost':
                if not os.path.exists(tar_remote):
                    return
                tar_local = tar_remote
            elif transport == 'kubectl':
                ns = self._namespace(node)
                subprocess.run(
                    ['kubectl', 'cp', f'{ns}/{target}:{tar_remote}', tar_local],
                    check=True)
            else:
                user = self._user_for(node)
                subprocess.run(
                    ['scp'] + self._ssh_args() +
                    [f'{user}@{target}:{tar_remote}', tar_local],
                    check=True)

            with tarfile.open(tar_local, 'r:gz') as tar:
                tar.extractall(path=local_dir, filter='data')
        finally:
            if os.path.exists(tar_local):
                os.unlink(tar_local)
            self.run_on(node, f'rm -f {tar_remote}', check=False)

    # --- Config generation ---

    def build_config_nodes(self):
        """Build nodes array for kvclient config from deploy.json nodes."""
        result = []
        for n in self.nodes:
            result.append({
                'host': self._comm_host(n),
                'port': n.get('port', self.listen_port),
                'instance_id': n['instance_id'],
            })
        return result

    def build_default_peers(self):
        port = self.listen_port
        return [f'http://{self._comm_host(n)}:{port}' for n in self.nodes]

    def build_peers(self, node):
        if 'peers' in node:
            return node['peers']
        port = self.listen_port
        my_id = node['instance_id']
        return [f'http://{self._comm_host(n)}:{port}'
                for n in self.nodes if n['instance_id'] != my_id]

    def build_node_overrides(self, node):
        override_keys = ('role', 'pipeline', 'notify_pipeline')
        return {k: v for k, v in node.items() if k in override_keys}

    def generate_config(self, node):
        config = dict(self.config_template)
        # Remove fields that deploy.py manages
        config.pop('instance_id', None)
        config.pop('nodes', None)
        config.pop('peers', None)
        # Inject from deploy.json
        config['instance_id'] = node['instance_id']
        config['nodes'] = self.build_config_nodes()
        config['peers'] = self.build_peers(node)
        config.update(self.build_node_overrides(node))
        return config

    # --- Actions ---

    def deploy_node(self, node):
        target = self._exec_target(node)
        instance_id = node['instance_id']
        transport = self._transport(node)

        print(f'Deploying to {target} (instance_id={instance_id}, transport={transport})...')

        config = self.generate_config(node)

        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.json', prefix=f'config_{instance_id}_',
            delete=False
        ) as tf:
            json.dump(config, tf, indent=2)
            tmp_config = tf.name

        try:
            self.run_on(node, f'mkdir -p {self.remote_work_dir}')

            remote_binary = f'{self.remote_work_dir}/kvclient_standalone_test'
            self.scp_to(node, self.binary_path, remote_binary)

            if os.path.isdir(self.datasystem_sdk_dir):
                print(f'  Deploying SDK libs to {target}...')
                self.run_on(node, f'rm -rf {self.remote_work_dir}/sdk_lib')
                self.scp_to(node, self.datasystem_sdk_dir, f'{self.remote_work_dir}/sdk_lib')

            remote_config = f'{self.remote_work_dir}/config_{instance_id}.json'
            self.scp_to(node, tmp_config, remote_config)

            self.run_on(node, f'chmod +x {self.remote_work_dir}/kvclient_standalone_test')

            if self.enable_procmon:
                procmon_src = os.path.join(
                    os.path.dirname(os.path.abspath(__file__)), 'procmon.py')
                self.scp_to(node, procmon_src, f'{self.remote_work_dir}/procmon.py')

            ld_path = f'{self.remote_work_dir}/sdk_lib' if os.path.isdir(self.datasystem_sdk_dir) else ''
            env_prefix = f'LD_LIBRARY_PATH={ld_path}:$LD_LIBRARY_PATH ' if ld_path else ''
            start_cmd = (
                f'cd {self.remote_work_dir} && '
                f'{env_prefix}'
                f'nohup ./kvclient_standalone_test config_{instance_id}.json '
                f'> stdout_{instance_id}.log 2>&1 &')
            self.run_on(node, start_cmd)

            if self.enable_procmon:
                procmon_cmd = (
                    f'cd {self.remote_work_dir} && '
                    f'nohup python3 procmon.py -p kvclient_standalone_test -i 2'
                    f' > procmon_{instance_id}.log 2>&1 &')
                self.run_on(node, procmon_cmd)

            print(f'  {target} -> OK')
            return True
        except Exception as e:
            print(f'  {target} -> FAILED: {e}')
            return False
        finally:
            os.unlink(tmp_config)

    def do_deploy(self):
        results = []
        with ThreadPoolExecutor(max_workers=len(self.nodes) or 1) as pool:
            futures = {pool.submit(self.deploy_node, n): n for n in self.nodes}
            for future in as_completed(futures):
                results.append(future.result())

        ok = sum(1 for r in results if r)
        total = len(results)
        print(f'\nDeploy result: {ok}/{total} succeeded')

    def do_stop(self):
        config = dict(self.config_template)
        config['peers'] = self.build_default_peers()

        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.json', prefix='config_full_',
            delete=False
        ) as tf:
            json.dump(config, tf, indent=2)
            tmp_config = tf.name

        try:
            ld_path = ''
            if os.path.isdir(self.datasystem_sdk_dir):
                ld_path = os.path.abspath(self.datasystem_sdk_dir)

            print('Stopping all instances...')
            env = os.environ.copy()
            if ld_path:
                env['LD_LIBRARY_PATH'] = f'{ld_path}:{env.get("LD_LIBRARY_PATH", "")}'
            subprocess.run(
                [self.binary_path, '--stop', tmp_config],
                env=env, check=True)

            # Stop procmon on all nodes
            if self.enable_procmon:
                for node in self.nodes:
                    self.run_on(node, 'pkill -f procmon.py', check=False)
        finally:
            os.unlink(tmp_config)

    def do_clean(self):
        results = []

        def clean_node(node):
            target = self._exec_target(node)
            print(f'Cleaning {target}...')
            try:
                self.run_on(
                    node,
                    f'pkill -f procmon.py; pkill -f kvclient_standalone_test; '
                    f'rm -rf {self.remote_work_dir}',
                    check=False)
                print(f'  {target} -> OK')
                return True
            except Exception as e:
                print(f'  {target} -> FAILED ({e})')
                return False

        with ThreadPoolExecutor(max_workers=len(self.nodes) or 1) as pool:
            futures = [pool.submit(clean_node, n) for n in self.nodes]
            for future in as_completed(futures):
                results.append(future.result())

        ok = sum(1 for r in results if r)
        print(f'\nClean result: {ok}/{len(results)}')

    def do_collect(self):
        collect_dir = 'collected'
        results = []

        def collect_node(node):
            instance_id = node['instance_id']
            target = self._exec_target(node)
            local_dir = os.path.join(collect_dir, f'{target}_{instance_id}')
            print(f'Collecting from {target} (instance_id={instance_id})...')
            try:
                self.collect_files(node, local_dir)
                count = len([f for f in os.listdir(local_dir)
                             if os.path.isfile(os.path.join(local_dir, f))])
                print(f'  {target} -> {count} files collected to {local_dir}/')
                return True
            except Exception as e:
                print(f'  {target} -> FAILED ({e})')
                return False

        with ThreadPoolExecutor(max_workers=len(self.nodes) or 1) as pool:
            futures = [pool.submit(collect_node, n) for n in self.nodes]
            for future in as_completed(futures):
                results.append(future.result())

        ok = sum(1 for r in results if r)
        print(f'\nCollect result: {ok}/{len(results)} -> {collect_dir}/')


def main():
    if len(sys.argv) < 3:
        print('Usage: deploy.py --deploy|--stop|--collect|--clean <deploy.json> '
              '[config_template.json]')
        print()
        print('  --deploy   Deploy binary + SDK libs + config to all nodes and start')
        print('  --stop     Stop all instances via kvclient_standalone_test --stop')
        print('  --collect  Collect output files from all nodes to collected/')
        print('  --clean    Kill processes and remove remote work dirs')
        sys.exit(1)

    action = sys.argv[1]
    deploy_json = sys.argv[2]
    config_template = (sys.argv[3] if len(sys.argv) > 3
                       else 'config/config.json.example')

    if action not in ('--deploy', '--stop', '--collect', '--clean'):
        print(f'Unknown action: {action}')
        sys.exit(1)

    deployer = Deployer(deploy_json, config_template)

    if action == '--deploy':
        deployer.do_deploy()
    elif action == '--stop':
        deployer.do_stop()
    elif action == '--collect':
        deployer.do_collect()
    elif action == '--clean':
        deployer.do_clean()


if __name__ == '__main__':
    main()
