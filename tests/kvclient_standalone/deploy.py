#!/usr/bin/env python3
"""Deploy kvclient_standalone_test to remote nodes via SSH or kubectl."""

import json
import os
import shutil
import subprocess
import sys
import tarfile
import tempfile
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed


class Deployer:
    def __init__(self, deploy_path, config_template_path):
        with open(deploy_path) as f:
            self.deploy = json.load(f)
        with open(config_template_path) as f:
            self.config_template = json.load(f)

        self.base_dir = os.path.dirname(os.path.abspath(__file__))
        self.nodes = self.deploy.get('nodes', [])
        self.remote_work_dir = self.deploy.get('remote_work_dir', '')
        self.binary_path = os.path.join(self.base_dir, 'kvclient_standalone_test')
        self.datasystem_sdk_dir = os.path.join(self.base_dir, 'lib')
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

    def run_on(self, node, cmd, check=True, timeout=60):
        """Run command on node via SSH, kubectl exec, or local shell."""
        transport = self._transport(node)
        target = self._exec_target(node)

        if transport == 'localhost':
            return subprocess.run(cmd, shell=True, check=check,
                                  capture_output=True, text=True, timeout=timeout)
        elif transport == 'kubectl':
            ns = self._namespace(node)
            return subprocess.run(
                ['kubectl', 'exec', target, '-n', ns, '--', 'sh', '-c', cmd],
                check=check, capture_output=True, text=True, timeout=timeout)
        else:
            user = self._user_for(node)
            return subprocess.run(
                ['ssh'] + self._ssh_args() + [f'{user}@{target}', cmd],
                check=check, capture_output=True, text=True, timeout=timeout)

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
                        tar.add(src, arcname=os.path.basename(dst))
                    remote_tar = f'{dst}.tar.gz'
                    subprocess.run(
                        ['kubectl', 'cp', tar_path, f'{ns}/{target}:{remote_tar}'],
                        check=True, timeout=120)
                    self.run_on(
                        node,
                        f'mkdir -p {os.path.dirname(dst)} && '
                        f'tar xzf {remote_tar} -C {os.path.dirname(dst)} && '
                        f'rm -f {remote_tar}')
                finally:
                    if os.path.exists(tar_path):
                        os.unlink(tar_path)
            else:
                subprocess.run(
                    ['kubectl', 'cp', src, f'{ns}/{target}:{dst}'],
                    check=True, timeout=120)
        else:
            user = self._user_for(node)
            subprocess.run(
                ['scp'] + self._ssh_args() + ['-r', src, f'{user}@{target}:{dst}'],
                check=True, timeout=120)

    def collect_files(self, node, local_dir):
        """Collect output files from node via tar."""
        iid = node['instance_id']
        tar_remote = f'/tmp/collect_{iid}.tar.gz'
        tar_local = f'/tmp/collect_{iid}.tar.gz'
        transport = self._transport(node)
        target = self._exec_target(node)

        os.makedirs(local_dir, exist_ok=True)

        # Clean stale tar from previous runs, then pack
        self.run_on(node, f'rm -f {tar_remote}', check=False)
        self.run_on(
            node,
            f'cd {self.remote_work_dir} && '
            f'{{ ls *.csv *.txt *.log 2>/dev/null && '
            f'tar czf {tar_remote} *.csv *.txt *.log; }} || true',
            check=False)

        # Check if tar was created on remote
        check = self.run_on(
            node, f'test -f {tar_remote}', check=False)
        if check.returncode != 0:
            print(f'  {target} -> no output files')
            return

        # Pull to local
        try:
            if transport == 'localhost':
                tar_local = tar_remote
            elif transport == 'kubectl':
                ns = self._namespace(node)
                subprocess.run(
                    ['kubectl', 'cp', f'{ns}/{target}:{tar_remote}', tar_local],
                    check=True, timeout=120)
            else:
                user = self._user_for(node)
                subprocess.run(
                    ['scp'] + self._ssh_args() +
                    [f'{user}@{target}:{tar_remote}', tar_local],
                    check=True, timeout=120)

            # Verify local file was actually created
            if not os.path.isfile(tar_local):
                print(f'  {target} -> kubectl cp returned 0 but no local file')
                return

            with tarfile.open(tar_local, 'r:gz') as tar:
                tar.extractall(path=local_dir, filter='data')
        finally:
            if transport != 'localhost' and os.path.exists(tar_local):
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

            remote_sdk = node.get('remote_sdk_dir', self.deploy.get('remote_sdk_dir', ''))
            if remote_sdk:
                print(f'  Using container SDK: {remote_sdk}')
            elif os.path.isdir(self.datasystem_sdk_dir):
                print(f'  Deploying SDK libs to {target}...')
                self.run_on(node, f'rm -rf {self.remote_work_dir}/lib')
                self.scp_to(node, self.datasystem_sdk_dir, f'{self.remote_work_dir}/lib')

            remote_config = f'{self.remote_work_dir}/config_{instance_id}.json'
            self.scp_to(node, tmp_config, remote_config)

            self.run_on(node, f'chmod +x {self.remote_work_dir}/kvclient_standalone_test')

            if self.enable_procmon:
                procmon_src = os.path.join(
                    os.path.dirname(os.path.abspath(__file__)), 'procmon.py')
                self.scp_to(node, procmon_src, f'{self.remote_work_dir}/procmon.py')

            if remote_sdk:
                ld_path = remote_sdk
            elif os.path.isdir(self.datasystem_sdk_dir):
                ld_path = f'{self.remote_work_dir}/lib'
            else:
                ld_path = ''
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
        if not os.path.isfile(self.binary_path):
            print(f'ERROR: binary not found: {self.binary_path}')
            print('  Run "build.sh" first to compile and package.')
            sys.exit(1)

        results = []
        with ThreadPoolExecutor(max_workers=len(self.nodes) or 1) as pool:
            futures = {pool.submit(self.deploy_node, n): n for n in self.nodes}
            for future in as_completed(futures):
                results.append(future.result())

        ok = sum(1 for r in results if r)
        total = len(results)
        print(f'\nDeploy result: {ok}/{total} succeeded')

    def do_stop(self):
        if not self.nodes:
            print('No nodes in deploy config')
            return

        print(f'Stopping {len(self.nodes)} instances...')

        def stop_one(node):
            transport = self._transport(node)
            port = node.get('port', self.listen_port)
            target = self._exec_target(node)

            if transport == 'kubectl':
                # Hit /stop from inside the pod via kubectl exec
                result = self.run_on(node,
                    f'python3 -c "'
                    f'from urllib.request import urlopen,Request;'
                    f'r=Request(\'http://localhost:{port}/stop\',data=b\'\',method=\'POST\');'
                    f'urlopen(r,timeout=3);print(\'ok\')"',
                    check=False)
                if result.returncode == 0 and 'ok' in (result.stdout or ''):
                    return (target, True)
                err = (result.stderr or result.stdout or '').strip()
                return (target, err or 'failed')
            else:
                host = self._comm_host(node)
                url = f'http://{host}:{port}'
                try:
                    req = urllib.request.Request(
                        url + '/stop', data=b'', method='POST')
                    with urllib.request.urlopen(req, timeout=5) as resp:
                        return (url, resp.status == 200)
                except Exception as e:
                    return (url, str(e))

        ok = 0
        with ThreadPoolExecutor(max_workers=len(self.nodes) or 1) as pool:
            futures = [pool.submit(stop_one, n) for n in self.nodes]
            for future in as_completed(futures):
                target, result = future.result()
                if result is True:
                    ok += 1
                    print(f'  {target} -> OK')
                else:
                    print(f'  {target} -> ERROR ({result})')

        print(f'Stop result: {ok}/{len(self.nodes)} succeeded')

        # Stop procmon on all nodes in parallel
        if self.enable_procmon:
            with ThreadPoolExecutor(max_workers=len(self.nodes) or 1) as pool:
                for f in as_completed(
                    [pool.submit(self.run_on, n, 'pkill -f procmon.py')
                     for n in self.nodes]):
                    pass

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
                if not os.path.isdir(local_dir):
                    return 'empty'
                count = len([f for f in os.listdir(local_dir)
                             if os.path.isfile(os.path.join(local_dir, f))])
                if count == 0:
                    print(f'  {target} -> 0 files')
                    return 'empty'
                print(f'  {target} -> {count} files collected to {local_dir}/')
                return 'ok'
            except Exception as e:
                print(f'  {target} -> FAILED ({e})')
                return 'fail'

        with ThreadPoolExecutor(max_workers=len(self.nodes) or 1) as pool:
            futures = [pool.submit(collect_node, n) for n in self.nodes]
            for future in as_completed(futures):
                results.append(future.result())

        ok = sum(1 for r in results if r == 'ok')
        empty = sum(1 for r in results if r == 'empty')
        fail = sum(1 for r in results if r == 'fail')
        total = len(results)
        print(f'\nCollect result: {ok} collected, {empty} empty, {fail} failed / {total} total -> {collect_dir}/')


def main():
    if len(sys.argv) < 3:
        print('Usage: deploy.py --deploy|--stop|--collect|--clean <deploy.json> '
              '[config_template.json]')
        print()
        print('  --deploy   Deploy binary + SDK libs + config to all nodes and start')
        print('  --stop     Stop all instances via HTTP /stop')
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
