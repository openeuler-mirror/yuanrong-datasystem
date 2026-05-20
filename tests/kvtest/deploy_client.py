#!/usr/bin/env python3
"""Deploy kvtest to remote nodes via SSH or kubectl."""

import json
import os
import shlex
import shutil
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
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
        self.binary_path = os.path.join(self.base_dir, 'build', 'kvtest')
        self.datasystem_sdk_dir = os.path.join(self.base_dir, 'lib')
        version_file = os.path.join(self.base_dir, 'VERSION')
        self.version = open(version_file).read().strip() if os.path.isfile(version_file) else '?'
        self.default_transport = self.deploy.get('transport', 'ssh')
        self.default_ssh_user = self.deploy.get('ssh_user', 'root')
        self.ssh_options = self.deploy.get('ssh_options', '-o StrictHostKeyChecking=no')
        self.enable_procmon = self.deploy.get('enable_procmon', True)
        self.listen_port = self.config_template.get('listen_port', 9000)
        self._host_locks = {}

    # --- Transport helpers ---

    def _ssh_args(self):
        return self.ssh_options.split() if self.ssh_options else []

    def _build_ssh_cmd(self, node):
        """Build base SSH command list with options and port."""
        cmd = ['ssh', '-T', '-n'] + self._ssh_args()
        port = node.get('ssh_port')
        if port:
            cmd += ['-p', str(port)]
        return cmd

    def _build_scp_cmd(self, node):
        """Build base SCP command list with options and port."""
        cmd = ['scp'] + self._ssh_args()
        port = node.get('ssh_port')
        if port:
            cmd += ['-P', str(port)]
        return cmd

    def _user_for(self, node):
        return node.get('ssh_user', self.default_ssh_user)

    def _transport(self, node):
        t = node.get('transport', self.default_transport)
        if t == 'ssh' and node.get('host') == 'localhost':
            return 'ssh'
        if node.get('host') == 'localhost':
            return 'localhost'
        return t

    def _exec_target(self, node):
        """Target for run_on / scp_to (IP for SSH, pod name for kubectl)."""
        if self._transport(node) == 'kubectl':
            return node.get('pod_name', '')
        return node.get('host', 'localhost')

    def _namespace(self, node):
        return node.get('namespace', 'default')

    def _comm_host(self, node):
        """Network-reachable host for kvclient inter-instance communication."""
        if node.get('comm_host'):
            return node['comm_host']
        if self._transport(node) == 'kubectl':
            return node.get('pod_ip', node.get('pod_name', ''))
        return node.get('host', 'localhost')

    # --- Transport primitives ---

    def run_on(self, node, cmd, check=True, timeout=60, allow_timeout=False):
        """Run command on node via SSH, kubectl exec, or local shell.

        If allow_timeout is True, return a dummy result instead of raising
        TimeoutExpired. Useful for fire-and-forget start commands.
        """
        transport = self._transport(node)
        target = self._exec_target(node)

        dummy = subprocess.CompletedProcess(args=[], returncode=0,
                                            stdout='', stderr='')

        def _run(sub_cmd, **kwargs):
            try:
                return subprocess.run(sub_cmd, **kwargs)
            except subprocess.TimeoutExpired:
                if allow_timeout:
                    return dummy
                raise

        if transport == 'localhost':
            return _run(cmd, shell=True, check=check,
                        capture_output=True, text=True, timeout=timeout)
        elif transport == 'kubectl':
            ns = self._namespace(node)
            kubectl_cmd = ['kubectl', 'exec', target, '-n', ns, '--', 'sh', '-c', cmd]
            print(f'  $ {" ".join(kubectl_cmd)}')
            return _run(kubectl_cmd,
                        check=check, capture_output=True, text=True, timeout=timeout)
        else:
            user = self._user_for(node)
            ssh_cmd = self._build_ssh_cmd(node) + [f'{user}@{target}', cmd]
            print(f'  $ {" ".join(ssh_cmd)}')
            return _run(ssh_cmd,
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
                self._build_scp_cmd(node) + ['-r', src, f'{user}@{target}:{dst}'],
                check=True, timeout=120)

    def collect_files(self, node, local_dir):
        """Collect output files from node."""
        transport = self._transport(node)
        target = self._exec_target(node)

        os.makedirs(local_dir, exist_ok=True)

        # Collect from metrics_* output directories + top-level logs
        ls = self.run_on(node,
            f'ls -d {self.remote_work_dir}/metrics_* 2>/dev/null',
            check=False)
        metrics_dirs = [d.strip() for d in (ls.stdout or '').splitlines() if d.strip()]

        files = []
        for mdir in metrics_dirs:
            fls = self.run_on(node,
                f'ls {mdir}/*.csv {mdir}/*.txt {mdir}/*.log 2>/dev/null',
                check=False)
            files.extend(f.strip() for f in (fls.stdout or '').splitlines() if f.strip())
        # Also collect top-level run.log and resource_monitor.log
        run_log = self.run_on(node,
            f'ls {self.remote_work_dir}/run.log {self.remote_work_dir}/resource_monitor.log 2>/dev/null',
            check=False)
        files.extend(f.strip() for f in (run_log.stdout or '').splitlines() if f.strip())
        if not files:
            print(f'  {target} -> no output files')
            return

        print(f'  {target} -> {len(files)} files')

        if transport == 'kubectl':
            # kubectl cp requires tar inside container; use cat instead
            ns = self._namespace(node)
            for remote_path in files:
                fname = os.path.basename(remote_path)
                local_path = os.path.join(local_dir, fname)
                cmd = ['kubectl', 'exec', target, '-n', ns, '--', 'cat', remote_path]
                try:
                    with open(local_path, 'wb') as f:
                        subprocess.run(cmd, stdout=f, check=True, timeout=30)
                except Exception as e:
                    print(f'    {fname} -> FAILED: {e}')
        elif transport == 'localhost':
            for remote_path in files:
                fname = os.path.basename(remote_path)
                local_path = os.path.join(local_dir, fname)
                shutil.copy2(remote_path, local_path)
        else:
            # SSH: tar + scp
            iid = node['instance_id']
            tar_remote = f'/tmp/collect_{iid}.tar.gz'
            tar_local = f'/tmp/collect_{iid}.tar.gz'
            self.run_on(node, f'rm -f {tar_remote}', check=False)
            self.run_on(node,
                f'cd {self.remote_work_dir} && '
                f'tar czf {tar_remote} metrics_* *.csv *.txt *.log run.log resource_monitor.log 2>/dev/null',
                check=False)
            check = self.run_on(node, f'test -f {tar_remote}', check=False)
            if check.returncode == 0:
                try:
                    user = self._user_for(node)
                    subprocess.run(
                        self._build_scp_cmd(node) +
                        [f'{user}@{target}:{tar_remote}', tar_local],
                        check=True, timeout=120)
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
        return [f'http://{self._comm_host(n)}:{n.get("port", self.listen_port)}' for n in self.nodes]

    def build_peers(self, node):
        if 'peers' in node:
            return node['peers']
        my_id = node['instance_id']
        return [f'http://{self._comm_host(n)}:{n.get("port", self.listen_port)}'
                for n in self.nodes if n['instance_id'] != my_id]

    def build_node_overrides(self, node):
        override_keys = ('role', 'pipeline', 'notify_pipeline', 'listen_port')
        return {k: v for k, v in node.items() if k in override_keys}

    def generate_config(self, node):
        config = dict(self.config_template)
        # Remove fields that deploy_client.py manages
        config.pop('instance_id', None)
        config.pop('nodes', None)
        config.pop('peers', None)
        # Keep 'env' field for runtime environment variables
        # Inject from deploy.json
        config['instance_id'] = node['instance_id']
        config['nodes'] = self.build_config_nodes()
        config['peers'] = self.build_peers(node)
        config.update(self.build_node_overrides(node))
        return config

    # --- Actions ---

    def _host_key(self, node):
        return (node.get('host', 'localhost'), node.get('ssh_port'))

    def _get_host_lock(self, node):
        key = self._host_key(node)
        return self._host_locks.setdefault(key, threading.Lock())

    def deploy_node(self, node):
        target = self._exec_target(node)
        instance_id = node['instance_id']
        transport = self._transport(node)
        tag = f'  [{target}:{instance_id}]'

        print(f'Deploying to {target} (instance_id={instance_id}, transport={transport})...')

        config = self.generate_config(node)
        role = config.get('role', 'writer')

        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.json', prefix=f'config_{instance_id}_',
            delete=False
        ) as tf:
            json.dump(config, tf, indent=2)
            tmp_config = tf.name

        try:
            # Step 1: Create remote directory
            print(f'{tag} mkdir {self.remote_work_dir}')
            self.run_on(node, f'mkdir -p {self.remote_work_dir}')

            # Step 2: Upload binary + SDK (under host lock to avoid concurrent races)
            remote_binary = f'{self.remote_work_dir}/kvtest'
            remote_sdk = node.get('remote_sdk_dir', self.deploy.get('remote_sdk_dir', ''))
            with self._get_host_lock(node):
                print(f'{tag} uploading binary ({os.path.getsize(self.binary_path) // 1024}KB)')
                self.scp_to(node, self.binary_path, remote_binary)
                if remote_sdk:
                    print(f'{tag} using container SDK: {remote_sdk}')
                elif os.path.isdir(self.datasystem_sdk_dir):
                    print(f'{tag} deploying SDK libs...')
                    self.run_on(node, f'rm -rf {self.remote_work_dir}/lib')
                    self.scp_to(node, self.datasystem_sdk_dir, f'{self.remote_work_dir}/lib')

            # Step 4: Upload config
            remote_config = f'{self.remote_work_dir}/config_{instance_id}.json'
            print(f'{tag} uploading config (role={role}, peers={len(config.get("peers", []))})')
            self.scp_to(node, tmp_config, remote_config)

            # Step 5: chmod
            self.run_on(node, f'chmod +x {self.remote_work_dir}/kvtest')

            # Step 6: Upload procmon
            if self.enable_procmon:
                script_dir = os.path.dirname(os.path.abspath(__file__))
                procmon_src = os.path.join(script_dir, 'procmon.py')
                if not os.path.exists(procmon_src):
                    procmon_src = os.path.join(script_dir, 'tools', 'procmon.py')
                self.scp_to(node, procmon_src, f'{self.remote_work_dir}/procmon.py')

            # Step 7: Start process
            if remote_sdk:
                ld_path = remote_sdk
            elif os.path.isdir(self.datasystem_sdk_dir):
                ld_path = f'{self.remote_work_dir}/lib'
            else:
                ld_path = ''
            env_prefix = f'LD_LIBRARY_PATH={ld_path}:$LD_LIBRARY_PATH ' if ld_path else ''

            # Add custom environment variables from config
            custom_env = config.get('env', {})
            if custom_env:
                env_parts = [f'export {k}={shlex.quote(str(v))}' for k, v in custom_env.items()]
                env_prefix += ' '.join(env_parts) + ' '

            start_cmd = (
                f"cd {self.remote_work_dir} && "
                f"{env_prefix}"
                f"nohup ./kvtest config_{instance_id}.json "
                f"> run.log 2>&1 </dev/null & "
                f"echo $!")
            print(f'{tag} starting kvclient (role={role})...')
            # allow_timeout: SSH nohup may not return in nested SSH environments,
            # but the remote process still starts. Verify with pgrep below.
            self.run_on(node, start_cmd, check=False, timeout=10, allow_timeout=True)

            # Step 8: Verify process started
            time.sleep(1)
            pid = None
            verify = self.run_on(
                node, f'pgrep -f "kvtest config_{instance_id}\\.json"',
                check=False)
            if verify.returncode == 0 and verify.stdout.strip():
                pid = verify.stdout.strip().split('\n')[0]
                print(f'{tag} process started (pid={pid})')
            else:
                print(f'{tag} WARNING: process not found after start, checking log...')
                log = self.run_on(
                    node, f'cat {self.remote_work_dir}/run.log 2>/dev/null',
                    check=False)
                if log.stdout.strip():
                    print(f'{tag} stdout: {log.stdout.strip()[:500]}')
                else:
                    print(f'{tag} stdout empty — binary may have crashed before any output')

            # Step 9: Start procmon
            if self.enable_procmon and pid:
                procmon_cmd = (
                    f"cd {self.remote_work_dir} && "
                    f"nohup python3 procmon.py --pid {pid} -i 2"
                    f" > resource_monitor.log 2>&1 </dev/null & disown")
                self.run_on(node, procmon_cmd, check=False, allow_timeout=True)

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

        print(f'Version: {self.version}')

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

        def http_stop(node):
            """Try HTTP POST /stop via curl, wget, then python3."""
            port = node.get('port', self.listen_port)
            url = f'http://localhost:{port}/stop'
            # Try curl first (most common in containers)
            r = self.run_on(node, f'curl -sf -X POST {url} --max-time 3', check=False, timeout=5)
            if r.returncode == 0:
                return True
            # Try wget
            r = self.run_on(node, f'wget -qO- --post-data="" --timeout=3 {url}', check=False, timeout=5)
            if r.returncode == 0:
                return True
            # Try python3
            r = self.run_on(node,
                f'python3 -c "'
                f'from urllib.request import urlopen,Request;'
                f'r=Request(\'http://localhost:{port}/stop\',data=b\'\',method=\'POST\');'
                f'urlopen(r,timeout=3);print(\'ok\')"',
                check=False, timeout=5)
            if r.returncode == 0 and 'ok' in (r.stdout or ''):
                return True
            return False

        def stop_one(node):
            target = self._exec_target(node)
            if http_stop(node):
                return (target, True)
            return (target, False)

        # Phase 1: graceful HTTP stop
        ok = 0
        with ThreadPoolExecutor(max_workers=len(self.nodes) or 1) as pool:
            futures = [pool.submit(stop_one, n) for n in self.nodes]
            for future in as_completed(futures):
                target, result = future.result()
                if result is True:
                    ok += 1
                    print(f'  {target} -> OK (graceful)')
                else:
                    print(f'  {target} -> HTTP stop failed')

        # Phase 2: wait for graceful shutdown (summary file generation, etc.)
        print('Waiting 5s for graceful shutdown...')
        time.sleep(5)

        # Phase 3: SIGTERM remaining processes
        def kill_remaining(node, sig=''):
            return self.run_on(
                node,
                f"for p in $(pgrep -f kvtest 2>/dev/null); do "
                f"kill {sig} $p 2>/dev/null; done; "
                f"for p in $(pgrep -f procmon.py 2>/dev/null); do "
                f"kill {sig} $p 2>/dev/null; done",
                check=False, timeout=10)

        def check_alive(node):
            r = self.run_on(node,
                'pgrep -f kvtest 2>/dev/null',
                check=False)
            return r.returncode == 0

        with ThreadPoolExecutor(max_workers=len(self.nodes) or 1) as pool:
            kill_futures = [pool.submit(kill_remaining, n) for n in self.nodes]
            for f in as_completed(kill_futures):
                pass

        time.sleep(2)

        # Phase 4: SIGKILL only nodes still alive
        alive = []
        with ThreadPoolExecutor(max_workers=len(self.nodes) or 1) as pool:
            check_futures = {pool.submit(check_alive, n): n for n in self.nodes}
            for f in as_completed(check_futures):
                if f.result():
                    alive.append(check_futures[f])

        if alive:
            print(f'Force killing {len(alive)} remaining processes...')
            with ThreadPoolExecutor(max_workers=len(alive)) as pool:
                kill9_futures = [pool.submit(kill_remaining, n, '-9') for n in alive]
                for f in as_completed(kill9_futures):
                    pass

        print(f'Stop result: {ok}/{len(self.nodes)} graceful, '
              f'{len(alive)} force killed')

    def do_clean(self):
        results = []

        def clean_node(node):
            target = self._exec_target(node)
            print(f'Cleaning {target}...')
            try:
                # Step 1: Kill processes (use pgrep+kill to avoid pkill -f matching own shell)
                self.run_on(node,
                    "for p in $(pgrep -f kvtest 2>/dev/null); do "
                    "kill $p 2>/dev/null; done; "
                    "for p in $(pgrep -f procmon.py 2>/dev/null); do "
                    "kill $p 2>/dev/null; done",
                    check=False, timeout=15)
                time.sleep(1)

                # Step 2: Force kill remaining
                self.run_on(node,
                    "for p in $(pgrep -f kvtest 2>/dev/null); do "
                    "kill -9 $p 2>/dev/null; done; "
                    "for p in $(pgrep -f procmon.py 2>/dev/null); do "
                    "kill -9 $p 2>/dev/null; done",
                    check=False, timeout=15)
                time.sleep(1)

                # Step 3: Remove directory
                self.run_on(node, f'rm -rf {self.remote_work_dir}',
                           check=False, timeout=15)

                # Verify cleanup
                verify = self.run_on(
                    node, f'ls {self.remote_work_dir} 2>/dev/null',
                    check=False)
                if verify.returncode == 0:
                    print(f'  {target} -> WARNING: dir still exists after clean')
                else:
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

        # Phase 1: trigger summary generation on running instances
        print('Triggering summary generation...')
        def trigger_summary(node):
            port = node.get('port', self.listen_port)
            url = f'http://localhost:{port}/summary'
            self.run_on(node, f'curl -sf -X POST {url} --max-time 3', check=False, timeout=5)

        with ThreadPoolExecutor(max_workers=len(self.nodes) or 1) as pool:
            futures = [pool.submit(trigger_summary, n) for n in self.nodes]
            for f in as_completed(futures):
                pass

        time.sleep(1)

        # Phase 2: collect files
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

    def do_run(self, duration):
        """Wait duration then auto stop + collect."""
        def fmt_duration(secs):
            if secs >= 3600:
                return f'{secs // 3600}h {secs % 3600 // 60}m {secs % 60}s'
            if secs >= 60:
                return f'{secs // 60}m {secs % 60}s'
            return f'{secs}s'

        print(f'\nRunning for {fmt_duration(duration)}, auto stop + collect after...')
        start = time.time()
        try:
            remaining = duration
            while remaining > 0:
                time.sleep(min(remaining, 60))
                elapsed = int(time.time() - start)
                remaining = duration - elapsed
                if remaining > 0:
                    print(f'  [{elapsed}/{duration}s elapsed, {remaining}s remaining]')
        except KeyboardInterrupt:
            elapsed = int(time.time() - start)
            print(f'\n  Interrupted after {elapsed}s, stopping early...')

        elapsed = int(time.time() - start)
        print(f'\n--- Run finished ({elapsed}s elapsed) ---')
        self.do_stop()
        self.do_collect()


def parse_duration(s):
    """Parse '30s', '5m', '2h', or bare number -> seconds."""
    s = str(s).strip()
    if s.endswith('h'):
        return int(s[:-1]) * 3600
    if s.endswith('m'):
        return int(s[:-1]) * 60
    if s.endswith('s'):
        return int(s[:-1])
    return int(s)


# --- gen-config ---

def _get_pods(namespace, prefix):
    try:
        out = subprocess.check_output(
            ['kubectl', 'get', 'pods', '-n', namespace, '-o', 'json',
             '--field-selector=status.phase=Running'],
            text=True, timeout=30)
    except FileNotFoundError:
        print('ERROR: kubectl not found', file=sys.stderr)
        sys.exit(1)
    except subprocess.CalledProcessError as e:
        print(f'ERROR: kubectl failed: {e.stderr}', file=sys.stderr)
        sys.exit(1)

    data = json.loads(out)
    pods = []
    for item in data.get('items', []):
        name = item['metadata']['name']
        if not name.startswith(prefix):
            continue
        pod_ip = item.get('status', {}).get('podIP', '')
        if not pod_ip:
            continue
        node_name = item.get('spec', {}).get('nodeName', '')
        pods.append({'name': name, 'ip': pod_ip, 'node': node_name})
    pods.sort(key=lambda p: p['name'])
    return pods


def _parse_pipeline(s):
    return [op.strip() for op in s.split(',') if op.strip()] if s else []


def cmd_gen_config(args):
    pods = _get_pods(args.namespace, args.prefix)
    if not pods:
        print(f'No running pods found with prefix "{args.prefix}" '
              f'in namespace "{args.namespace}"')
        sys.exit(1)

    if args.writer_count < 0 or args.writer_count > len(pods):
        print(f'ERROR: --writer-count ({args.writer_count}) must be 0..{len(pods)}',
              file=sys.stderr)
        sys.exit(1)

    node_pods = {}
    for i, pod in enumerate(pods):
        node_pods.setdefault(pod['node'], []).append(i)

    writer_indices = set()
    sorted_nodes = sorted(node_pods.keys())
    pod_queues = {n: list(node_pods[n]) for n in sorted_nodes}
    assigned = 0
    while assigned < args.writer_count:
        for node in sorted_nodes:
            if assigned >= args.writer_count:
                break
            if pod_queues[node]:
                writer_indices.add(pod_queues[node].pop(0))
                assigned += 1

    writer_pipeline = _parse_pipeline(args.pipeline)
    notify_pipeline = _parse_pipeline(args.notify_pipeline)

    nodes = []
    for i, pod in enumerate(pods):
        is_writer = i in writer_indices
        node = {
            'pod_name': pod['name'],
            'pod_ip': pod['ip'],
            'namespace': args.namespace,
            'instance_id': i,
            'role': 'writer' if is_writer else 'reader',
            'pipeline': writer_pipeline if is_writer else [],
            'notify_pipeline': notify_pipeline,
        }
        if args.batch_keys_count > 1:
            node['batch_keys_count'] = args.batch_keys_count
        nodes.append(node)

    deploy = {
        'remote_work_dir': args.remote_work_dir,
        'transport': 'kubectl',
        'enable_procmon': True,
        'nodes': nodes,
    }
    if args.remote_sdk_dir:
        deploy['remote_sdk_dir'] = args.remote_sdk_dir

    os.makedirs(args.output_dir, exist_ok=True)

    deploy_path = os.path.join(args.output_dir, 'deploy.json')
    with open(deploy_path, 'w') as f:
        json.dump(deploy, f, indent=2)
        f.write('\n')
    print(f'Generated {deploy_path} ({len(nodes)} pods)')

    script_dir = os.path.dirname(os.path.abspath(__file__))
    src = os.path.join(script_dir, 'config', 'config.json.example')
    dst = os.path.join(args.output_dir, 'config.json')
    if os.path.exists(src):
        shutil.copy2(src, dst)
        overrides = {}
        if args.etcd_address:
            overrides['etcd_address'] = args.etcd_address
        if args.cluster_name:
            overrides['cluster_name'] = args.cluster_name
        if writer_pipeline:
            overrides['pipeline'] = writer_pipeline
        if notify_pipeline:
            overrides['notify_pipeline'] = notify_pipeline
        if args.batch_keys_count > 1:
            overrides['batch_keys_count'] = args.batch_keys_count
        if args.key_pool_size > 0:
            overrides['key_pool_size'] = args.key_pool_size
        if args.target_hit_rate > 0:
            overrides['target_hit_rate'] = args.target_hit_rate
        if args.warmup_timeout != 60:
            overrides['warmup_timeout_seconds'] = args.warmup_timeout
        if args.inference_delay > 0:
            overrides['inference_delay_ms'] = args.inference_delay
        if overrides:
            with open(dst) as f:
                cfg = json.load(f)
            cfg.update(overrides)
            with open(dst, 'w') as f:
                json.dump(cfg, f, indent=2)
                f.write('\n')
            for k, v in overrides.items():
                print(f'  {k} set to {v}')
        print(f'Copied {src} -> {dst}')
    else:
        print(f'WARNING: {src} not found, skipping config.json')

    for node in nodes:
        print(f'  {node["pod_name"]} -> {node["pod_ip"]} '
              f'(instance_id={node["instance_id"]}, role={node["role"]})')


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description='Deploy kvtest to remote nodes via SSH or kubectl.')
    sub = parser.add_subparsers(dest='command')

    # deploy
    p = sub.add_parser('deploy', help='Deploy + start (auto stop+collect if duration set)')
    p.add_argument('deploy_json', help='Path to deploy.json')
    p.add_argument('config_template', nargs='?', default='config/config.json.example',
                   help='Config template (default: config/config.json.example)')

    # stop
    p = sub.add_parser('stop', help='Stop all instances via HTTP /stop')
    p.add_argument('deploy_json')
    p.add_argument('config_template', nargs='?', default='config/config.json.example')

    # collect
    p = sub.add_parser('collect', help='Collect output files to collected/')
    p.add_argument('deploy_json')
    p.add_argument('config_template', nargs='?', default='config/config.json.example')

    # clean
    p = sub.add_parser('clean', help='Kill processes and remove remote work dirs')
    p.add_argument('deploy_json')
    p.add_argument('config_template', nargs='?', default='config/config.json.example')

    # gen-config
    p = sub.add_parser('gen-config',
                       help='Generate deploy.json from k8s Pod names')
    p.add_argument('-p', '--prefix', required=True,
                   help='Pod name prefix to match')
    p.add_argument('-n', '--namespace', default='default',
                   help='k8s namespace (default: default)')
    p.add_argument('-r', '--remote-work-dir',
                   default='/home/user/kvclient_test',
                   help='Remote work directory')
    p.add_argument('-o', '--output-dir', default='config',
                   help='Output directory (default: config)')
    p.add_argument('-e', '--etcd-address',
                   help='Override etcd_address in generated config.json')
    p.add_argument('-c', '--cluster-name',
                   help='Set cluster_name in generated config.json')
    p.add_argument('--remote-sdk-dir',
                   help='SDK lib path inside containers')
    p.add_argument('-w', '--writer-count', type=int, default=1,
                   help='Number of writer instances (default: 1)')
    p.add_argument('--pipeline', default='setStringView',
                   help='Comma-separated writer pipeline ops (default: setStringView)')
    p.add_argument('--notify-pipeline', default='getBuffer',
                   help='Comma-separated notify pipeline ops (default: getBuffer)')
    p.add_argument('--batch-keys-count', type=int, default=1,
                   help='batch_keys_count for batch ops (default: 1)')
    p.add_argument('--key-pool-size', type=int, default=0,
                   help='Cache mode key pool size (0 = disabled)')
    p.add_argument('--target-hit-rate', type=float, default=0,
                   help='Target cache hit rate 0.01~1.0 (0 = no adjustment)')
    p.add_argument('--warmup-timeout', type=int, default=60,
                   help='Reader warmup timeout in seconds (default: 60)')
    p.add_argument('--inference-delay', type=int, default=0,
                   help='Reader inference delay in ms (default: 0)')

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)

    if args.command == 'gen-config':
        cmd_gen_config(args)
        return

    deployer = Deployer(args.deploy_json, args.config_template)

    if args.command == 'deploy':
        deployer.do_deploy()
        duration = parse_duration(deployer.deploy.get('duration', '0'))
        if duration > 0:
            deployer.do_run(duration)
    elif args.command == 'stop':
        deployer.do_stop()
    elif args.command == 'collect':
        deployer.do_collect()
    elif args.command == 'clean':
        deployer.do_clean()


if __name__ == '__main__':
    main()
