#!/usr/bin/env python3
"""Deploy kvtest to remote nodes via SSH or kubectl."""

import json
import os
import posixpath
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

from log_collect import (add_collect_filters, archive_command, filters_from_args, has_filters,
                         pod_directory, receive_archive, select_targets, copy_case_files, archive_options_from_args,
                         host_selection_from_args, filter_collect_targets, read_pod_addresses)

from deploy_common import (
    _print_timings,
    get_pods,
    log_error,
    log_info,
    normalize_jemalloc_prof_conf,
    setup_logging,
)


_GRACEFUL_STOP_TIMEOUT = 120
_SUMMARY_TIMEOUT = 60
_POLL_INTERVAL = 2


class Deployer:
    def __init__(self, deploy_path, config_template_path=None):
        self.case_config_paths = [os.path.abspath(deploy_path)]
        if config_template_path:
            self.case_config_paths.append(os.path.abspath(config_template_path))
        with open(deploy_path) as f:
            self.deploy = json.load(f)
        # config_template is optional: install does not need it, only start /
        # deploy / gen-config do. When None, self.config_template is {} and
        # any caller that needs it (start_node via generate_config) will
        # raise a clear error on missing keys rather than silently using {}.
        if config_template_path:
            with open(config_template_path) as f:
                self.config_template = json.load(f)
        else:
            self.config_template = {}

        self.base_dir = os.path.dirname(os.path.abspath(__file__))
        self.nodes = self.deploy.get('nodes', [])
        self.remote_work_dir = self.deploy.get('remote_work_dir', '')
        self.binary_path = None  # resolved in main() via --kvtest-binary-path or deploy.json
        self.jemalloc_prof_conf = None
        version_file = os.path.join(self.base_dir, 'VERSION')
        self.version = open(version_file).read().strip() if os.path.isfile(version_file) else '?'
        self.default_transport = self.deploy.get('transport', 'ssh')
        self.default_ssh_user = self.deploy.get('ssh_user', 'root')
        self.ssh_options = self.deploy.get('ssh_options', '-o StrictHostKeyChecking=no')
        self.enable_procmon = self.deploy.get('enable_procmon', False)
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
        if node.get('host') == 'localhost':
            return 'localhost'
        t = node.get('transport', self.default_transport)
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
            log_info(f'  $ {" ".join(kubectl_cmd)}')
            return _run(kubectl_cmd,
                        check=check, capture_output=True, text=True, timeout=timeout)
        else:
            user = self._user_for(node)
            ssh_cmd = self._build_ssh_cmd(node) + [f'{user}@{target}', cmd]
            log_info(f'  $ {" ".join(ssh_cmd)}')
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

    @staticmethod
    def _local_path_for(remote_path, local_dir, remote_dir):
        """Map a remote path to a local path, preserving subpath under remote_dir.

        Mirrors `tar` extraction semantics so that files with the same basename
        but different directories (e.g. ``run.log`` and ``metrics_*/run.log``)
        do not overwrite each other in ``local_dir``.

        Falls back to ``os.path.basename`` when ``remote_path`` is not strictly
        under ``remote_dir`` (escapes the dir, sits on a different drive, equals
        ``remote_dir`` itself, or ``remote_dir`` is None).
        """
        rel = None
        if remote_dir:
            try:
                candidate = os.path.relpath(remote_path, remote_dir)
            except ValueError:
                # Different drive on Windows — cannot compute relpath.
                candidate = None
            if candidate is not None:
                # Normalize so '.', '..', trailing slashes etc. are canonical
                # before the escape check.
                candidate = os.path.normpath(candidate)
                if (candidate == '.'
                        or candidate == '..'
                        or candidate.startswith('..' + os.sep)
                        or os.path.isabs(candidate)):
                    # remote_path is remote_dir itself, escapes remote_dir,
                    # or is absolute and unrelated — fall back to basename.
                    rel = None
                else:
                    rel = candidate
        if rel is None:
            rel = os.path.basename(remote_path)
        local_path = os.path.join(local_dir, rel)
        parent = os.path.dirname(local_path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        return local_path

    def _collect_remote_files(self, node, local_dir, files, file_label='files', remote_dir=None,
                              tar_pattern=None, archive_options=None):
        """Collect remote files to local directory (internal helper)."""
        transport = self._transport(node)
        target = self._exec_target(node)

        os.makedirs(local_dir, exist_ok=True)

        if not files:
            log_info(f'  {target} -> no {file_label}')
            return

        log_info(f'  {target} -> {len(files)} {file_label}')

        if archive_options is not None:
            root = remote_dir or '/'
            relative_files = [posixpath.relpath(path, root) for path in files]
            if any(path == '..' or path.startswith('../') for path in relative_files):
                raise ValueError('Collected file is outside the remote log directory')
            command = shlex.join(['tar', 'czf' if archive_options['compress'] else 'cf', '-',
                                 '-C', root, '--'] + relative_files)
            if transport == 'kubectl':
                command = ['kubectl', 'exec', target, '-n', self._namespace(node), '--', 'sh', '-c', command]
            elif transport != 'localhost':
                command = self._build_ssh_cmd(node) + [f'{self._user_for(node)}@{target}', command]
            name = 'sdk' if file_label == 'SDK log files' else 'output'
            return receive_archive(command, local_dir, shell=transport == 'localhost',
                                   archive_options=archive_options, archive_name=name)

        if transport == 'kubectl':
            # Stream all files in one kubectl exec (tar cf - | local tar xf -).
            # No gzip by default (faster in practice); --compress adds gzip.
            # Packs relative paths (tar -C root) so extraction preserves the
            # same subpath layout as the --compress branch.
            ns = self._namespace(node)
            root = remote_dir or '/'
            relative_files = [posixpath.relpath(path, root) for path in files]
            if any(path == '..' or path.startswith('../') for path in relative_files):
                log_info(f'    {target} -> collected file outside remote dir; falling back to cat')
            else:
                tar_args = shlex.join(['tar', 'cf', '-', '-C', root, '--'] + relative_files)
                command = ['kubectl', 'exec', target, '-n', ns, '--', 'sh', '-c', tar_args]
                try:
                    count = receive_archive(command, local_dir, timeout=120,
                                            archive_options=dict(compress=False, extract=True))
                    if count > 0:
                        return
                except Exception as e:
                    log_info(f'    {target} -> tar stream failed: {e}; falling back to cat')

            # Fallback: per-file cat (for containers without tar).
            for remote_path in files:
                local_path = self._local_path_for(remote_path, local_dir, remote_dir)
                cmd = ['kubectl', 'exec', target, '-n', ns, '--', 'cat', remote_path]
                try:
                    with open(local_path, 'wb') as f:
                        subprocess.run(cmd, stdout=f, check=True, timeout=120)
                except Exception as e:
                    log_info(f'    {remote_path} -> {local_path} FAILED: {e}')
        elif transport == 'localhost':
            for remote_path in files:
                local_path = self._local_path_for(remote_path, local_dir, remote_dir)
                shutil.copy2(remote_path, local_path)
        else:
            # SSH: tar + scp
            iid = node['instance_id']
            tar_suffix = file_label.replace(' ', '_')
            tar_remote = f'/tmp/collect_{tar_suffix}_{iid}.tar.gz'
            tar_local = f'/tmp/collect_{tar_suffix}_{iid}.tar.gz'
            self.run_on(node, f'rm -f {tar_remote}', check=False)
            if remote_dir and tar_pattern:
                self.run_on(node,
                            f'cd {remote_dir} && '
                            f'tar czf {tar_remote} {tar_pattern} 2>/dev/null',
                            check=False)
            else:
                # If no remote_dir and tar_pattern, use individual files
                file_list = ' '.join(shlex.quote(f) for f in files)
                self.run_on(node,
                            f'tar czf {tar_remote} {file_list} 2>/dev/null',
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

    def collect_files(self, node, local_dir, archive_options=None):
        """Collect output files from node."""
        target = self._exec_target(node)

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
        # Also collect top-level run.log and resource_monitor.csv
        run_log = self.run_on(node,
                              f'ls {self.remote_work_dir}/run.log {self.remote_work_dir}/resource_monitor.csv 2>/dev/null',
                              check=False)
        files.extend(f.strip() for f in (run_log.stdout or '').splitlines() if f.strip())

        self._collect_remote_files(
            node, local_dir, files,
            file_label='output files',
            remote_dir=self.remote_work_dir,
            tar_pattern='metrics_* *.csv *.txt *.log run.log resource_monitor.csv',
            **({'archive_options': archive_options} if archive_options is not None else {})
        )

    def collect_sdk_logs(self, node, local_dir, sdk_log_dir='/root/.datasystem/logs', archive_options=None):
        """Collect SDK logs from node."""
        target = self._exec_target(node)

        # Collect all log files from SDK log directory
        ls = self.run_on(node,
                         f'ls -d {sdk_log_dir} 2>/dev/null',
                         check=False)
        if ls.returncode != 0:
            log_info(f'  {target} -> SDK log dir {sdk_log_dir} does not exist')
            return

        fls = self.run_on(node,
                          f'ls {sdk_log_dir}/*.log {sdk_log_dir}/*.log.gz {sdk_log_dir}/*.txt 2>/dev/null',
                          check=False)
        files = [f.strip() for f in (fls.stdout or '').splitlines() if f.strip()]

        self._collect_remote_files(
            node, local_dir, files,
            file_label='SDK log files',
            remote_dir=sdk_log_dir,
            tar_pattern='*.log *.log.gz *.txt',
            **({'archive_options': archive_options} if archive_options is not None else {})
        )

    # --- Config generation ---

    def build_config_nodes(self):
        """Build nodes array for kvclient config from deploy.json nodes."""
        result = []
        for n in self.nodes:
            result.append({
                'host': self._comm_host(n),
                'port': n.get('port', self.listen_port),
                'instance_id': n['instance_id'],
                'role': n.get('role', 'writer'),
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
        override_keys = ('role', 'pipeline', 'notify_pipeline', 'listen_port', 'cuda', 'numa_node')
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
        if 'numa_node' in node:
            config['random_numa_node'] = False
        if 'cuda' in node:
            config['cuda'] = dict(self.config_template.get('cuda', {}), **node['cuda'])
        if self.jemalloc_prof_conf is not None and not config.get('output_dir'):
            config['output_dir'] = f'metrics_{node["instance_id"]}_{time.strftime("%Y%m%d_%H%M%S")}'
        return config

    # --- Actions ---

    def _host_key(self, node):
        return (node.get('host', 'localhost'), node.get('ssh_port'))

    def _get_host_lock(self, node):
        key = self._host_key(node)
        return self._host_locks.setdefault(key, threading.Lock())

    def prepare_jemalloc_prof_env(self, node, config, ld_path, custom_env):
        conf, parent = normalize_jemalloc_prof_conf(
            self.jemalloc_prof_conf, config['output_dir'], node['instance_id'])
        probe_env = dict(custom_env, MALLOC_CONF='')
        existing_lib_path = probe_env.pop('LD_LIBRARY_PATH', None)
        env_prefix = ' '.join(f'{key}={shlex.quote(str(value))}' for key, value in probe_env.items())
        if existing_lib_path is not None:
            combined = f'{ld_path}:{existing_lib_path}' if ld_path else str(existing_lib_path)
            env_prefix += f' LD_LIBRARY_PATH={shlex.quote(combined)}'
        elif ld_path:
            env_prefix += f' LD_LIBRARY_PATH={shlex.quote(ld_path)}:"${{LD_LIBRARY_PATH:-}}"'
        result = self.run_on(
            node, f'cd {shlex.quote(self.remote_work_dir)} && '
            f'{env_prefix} ./kvtest --version', check=False)
        if (result.returncode != 0
                or 'jemalloc_prof_supported=true' not in result.stdout.splitlines()):
            raise RuntimeError(
                'kvtest does not support jemalloc profiling or its runtime cannot be loaded; '
                'rebuild with build.sh -b bazel -x on and deploy the matching lib/ directory')
        directory = shlex.quote(parent)
        self.run_on(
            node, f'cd {shlex.quote(self.remote_work_dir)} && '
            f'mkdir -p -- {directory} && test -d {directory} && '
            f'test -w {directory} && test -x {directory}')
        custom_env['MALLOC_CONF'] = conf

    def install_node(self, node):
        """Install binary + .so + procmon + launcher to a node (no start).

        Idempotent: re-running overwrites files but does not touch any
        running process. Split out of the legacy ``deploy_node`` so a
        large cluster can finish all uploads before any process starts,
        avoiding the upload-order-spreads-start-time skew on 2000+ nodes.
        Returns ``(ok, elapsed)`` where ``elapsed`` is the upload wall-clock
        (binary + lib + scripts); no per-step timing is exposed.
        """
        target = self._exec_target(node)
        instance_id = node['instance_id']
        transport = self._transport(node)
        tag = f'  [{target}:{instance_id}]'

        log_info(f'Installing to {target} (instance_id={instance_id}, transport={transport})...')

        t0 = time.monotonic()
        try:
            # Step 1: Create remote directory
            log_info(f'{tag} mkdir {self.remote_work_dir}')
            self.run_on(node, f'mkdir -p -- {shlex.quote(self.remote_work_dir)}')

            # Step 2: Upload binary + .so (under host lock to avoid concurrent
            # scp races on the same target file when multiple instances share
            # a host).
            remote_binary = f'{self.remote_work_dir}/kvtest'
            remote_sdk = node.get('remote_sdk_dir', self.deploy.get('remote_sdk_dir', ''))
            local_lib_dir = os.path.join(os.path.dirname(os.path.abspath(self.binary_path)), 'lib')
            bundled_jemalloc = os.path.join(local_lib_dir, 'libjemalloc.so.2')
            has_bundled_jemalloc = os.path.isfile(bundled_jemalloc)
            with self._get_host_lock(node):
                log_info(f'{tag} uploading binary ({os.path.getsize(self.binary_path) // 1024}KB)')
                self.scp_to(node, self.binary_path, remote_binary)
                if remote_sdk:
                    log_info(f'{tag} using container SDK: {remote_sdk}')
                else:
                    if os.path.isdir(local_lib_dir):
                        import glob as _glob
                        so_files = _glob.glob(os.path.join(local_lib_dir, '*.so*'))
                        if so_files:
                            remote_lib = f'{self.remote_work_dir}/lib'
                            log_info(f'{tag} uploading lib ({len(so_files)} .so files)')
                            self.run_on(node, f'mkdir -p -- {shlex.quote(remote_lib)}')
                            for so_file in so_files:
                                if so_file != bundled_jemalloc:
                                    self.scp_to(node, so_file, f'{remote_lib}/{os.path.basename(so_file)}')
                            remote_sdk = remote_lib

                if has_bundled_jemalloc:
                    allocator_dir = f'{self.remote_work_dir}/allocator_lib'
                    directory = shlex.quote(allocator_dir)
                    self.run_on(node, f'rm -rf -- {directory} && mkdir -p -- {directory}')
                    self.scp_to(node, bundled_jemalloc, f'{allocator_dir}/libjemalloc.so.2')

            # Step 3: chmod
            self.run_on(node, f'chmod +x {shlex.quote(remote_binary)}')

            # Step 4: Upload procmon (one-time; start phase does not re-upload)
            if self.enable_procmon:
                script_dir = os.path.dirname(os.path.abspath(__file__))
                procmon_src = os.path.join(script_dir, 'procmon.py')
                if not os.path.exists(procmon_src):
                    procmon_src = os.path.join(script_dir, 'tools', 'procmon.py')
                self.scp_to(node, procmon_src, f'{self.remote_work_dir}/procmon.py')

            # Step 5: Upload standalone_launcher.py (fork+setsid via Python
            # syscall, not the `setsid` binary which may be missing on minimal
            # images). Used by start phase to launch kvtest detached from the
            # caller's session so kubectl exec / ssh returns promptly instead
            # of hanging for the full subprocess timeout on a nohup-backgrounded
            # process. If upload fails, start phase falls back to nohup.
            script_dir = os.path.dirname(os.path.abspath(__file__))
            launcher_src = os.path.join(script_dir, 'standalone_launcher.py')
            if not os.path.exists(launcher_src):
                launcher_src = os.path.join(script_dir, 'tools',
                                            'standalone_launcher.py')
            if os.path.exists(launcher_src):
                try:
                    self.scp_to(node, launcher_src,
                                f'{self.remote_work_dir}/standalone_launcher.py')
                except Exception as e:
                    log_info(f'{tag} WARNING: launcher upload failed: {e}; '
                             f'start phase will fall back to nohup path')

            log_info(f'  {target} -> OK')
            return True, time.monotonic() - t0
        except Exception as e:
            log_info(f'  {target} -> FAILED: {e}')
            return False, time.monotonic() - t0

    def start_node(self, node, keep_remote_config=False):
        """Start kvtest on a node (assumes install_node already ran).

        When ``keep_remote_config`` is False (default), generates and uploads
        the per-node config, then launches the binary via
        standalone_launcher.py (nohup fallback). When True, skips the
        generate+upload step and reuses the remote ``config_{instance_id}.json``
        already placed by a prior ``deploy`` — used for rolling upgrades where
        the deploy.json is split into batches but the per-node config (which
        contains the full peer topology) must remain intact. ``env`` block
        and ``host_id_env_name`` are still read from the local
        ``config_template`` so launcher-side HOST_IP injection stays correct.

        Does NOT upload the binary, .so, procmon.py, or launcher.py — install
        phase owns those. Returns ``(ok, start_elapsed)`` where
        ``start_elapsed`` is the launcher-reported Popen→ready elapsed when
        available (excludes kubectl exec / ssh overhead), else the outer
        wall-clock.
        """
        target = self._exec_target(node)
        instance_id = node['instance_id']
        tag = f'  [{target}:{instance_id}]'

        remote_config = f'{self.remote_work_dir}/config_{instance_id}.json'

        if keep_remote_config:
            # Reuse the remote config left by a prior deploy. We still need
            # the env block + host_id_env_name from the local template so the
            # launcher injects HOST_IP correctly; everything else (peers,
            # nodes topology) stays as previously deployed.
            config = dict(self.config_template)
            config['role'] = node.get('role', config.get('role', 'writer'))
            role = config.get('role', 'writer')

            # Pre-flight: the remote config must exist. A missing file means
            # either install never ran, clean-logs wiped it, or instance_id
            # changed — fail loudly instead of letting the launcher error out.
            check_cfg = self.run_on(
                node, f'test -f {shlex.quote(remote_config)}',
                check=False, timeout=10)
            if check_cfg.returncode != 0:
                log_info(f'{tag} FAILED: remote config not found at '
                         f'{remote_config}; run a full deploy first '
                         f'(keep_remote_config requires prior config upload)')
                return False, 0.0

            log_info(f'{tag} reusing remote config (role={role})')
            tmp_config = None
        else:
            config = self.generate_config(node)
            role = config.get('role', 'writer')

            with tempfile.NamedTemporaryFile(
                mode='w', suffix='.json', prefix=f'config_{instance_id}_',
                delete=False
            ) as tf:
                json.dump(config, tf, indent=2)
                tmp_config = tf.name

            # Upload config (small file; filename contains instance_id so
            # same-host instances do not collide — no host_lock needed).
            log_info(f'{tag} uploading config (role={role}, peers={len(config.get("peers", []))})')
            self.scp_to(node, tmp_config, remote_config)

        try:

            # Resolve SDK lib path (set by install phase on the node).
            remote_sdk = node.get('remote_sdk_dir', self.deploy.get('remote_sdk_dir', ''))
            local_lib_dir = os.path.join(os.path.dirname(os.path.abspath(self.binary_path)), 'lib')
            if not remote_sdk:
                # Match install_node's fallback: if .so was uploaded to lib/
                if os.path.isdir(local_lib_dir):
                    import glob as _glob
                    if _glob.glob(os.path.join(local_lib_dir, '*.so*')):
                        remote_sdk = f'{self.remote_work_dir}/lib'
            ld_path = remote_sdk if remote_sdk else ''
            has_bundled_jemalloc = os.path.isfile(os.path.join(local_lib_dir, 'libjemalloc.so.2'))
            if has_bundled_jemalloc:
                ld_path = f'{self.remote_work_dir}/allocator_lib' + (f':{ld_path}' if ld_path else '')

            # Custom env vars: HOST_IP injection + SDK tuning.
            # Uses status.hostIP (k8s node InternalIP), NOT nodeName (hostname)
            # — hostname would break coordinator/etcd registration which
            # expects an IP address.
            custom_env = {k: v for k, v in config.get('env', {}).items() if k}
            if ld_path and 'LD_LIBRARY_PATH' in custom_env:
                ld_path += ':' + str(custom_env.pop('LD_LIBRARY_PATH'))
            host_id_env = config.get('host_id_env_name') or 'HOST_IP'
            if host_id_env and host_id_env not in custom_env:
                host_ip = node.get('host_ip', '')
                if not host_ip:
                    raise RuntimeError(
                        f'{host_id_env} is empty: pod {node.get("pod_name", "?")} '
                        f'(node {node.get("host", "?")}) has no status.hostIP; '
                        f'cannot inject a valid node IP — check k8s node status')
                custom_env[host_id_env] = host_ip
            # SDK reads DATASYSTEM_UB_GET_DATA_SIZE_BYTES at client init (default 32MB);
            # kvtest workloads fit in 10MB. Overridable via config 'env'.
            if 'DATASYSTEM_UB_GET_DATA_SIZE_BYTES' not in custom_env:
                custom_env['DATASYSTEM_UB_GET_DATA_SIZE_BYTES'] = '10485760'

            # Pre-flight: binary must exist (install must have run). Skip the
            # node with a clear error instead of attempting a doomed launch.
            check_bin = self.run_on(
                node, f'test -x {self.remote_work_dir}/kvtest', check=False, timeout=10)
            if check_bin.returncode != 0:
                log_info(f'{tag} FAILED: kvtest binary not found or not executable '
                         f'at {self.remote_work_dir}/kvtest; run install first')
                return False, 0.0

            # Skip if already running (idempotent start; matches coordinator
            # cmd_start skip-alive semantics).
            already = self.run_on(node, 'pgrep -x kvtest', check=False, timeout=10)
            if already.returncode == 0 and already.stdout.strip():
                if self.jemalloc_prof_conf is not None:
                    raise RuntimeError('profiling configuration requires stopping and restarting the existing kvtest')
                log_info(f'{tag} already running (pid={already.stdout.strip().split(chr(10))[0]}), skip')
                return True, 0.0

            if self.jemalloc_prof_conf is not None:
                self.prepare_jemalloc_prof_env(node, config, ld_path, custom_env)

            # Launch via standalone_launcher.py (fork+setsid via Python syscall).
            # The launcher parent prints "{pid} {elapsed}" to stdout and exits
            # when the binary is ready, so kubectl exec / ssh returns promptly
            # instead of hanging on the SPDY pipe held by a nohup-backgrounded
            # binary. Falls back to nohup only if the launcher script is absent
            # (install phase upload failed) — detected per-node via test -f.
            log_info(f'{tag} starting kvclient (role={role})...')
            t_start = time.monotonic()
            pid = None
            launch_elapsed = None
            launcher_check = self.run_on(
                node, f'test -f {self.remote_work_dir}/standalone_launcher.py',
                check=False, timeout=5)
            use_launcher = launcher_check.returncode == 0
            try:
                if use_launcher:
                    # Custom env vars (HOST_IP etc.) are set in the shell prefix
                    # so the launcher inherits them via os.environ; LD_LIBRARY_PATH
                    # is handled by the launcher's --lib-path arg. No --port:
                    # kvtest client doesn't listen; the launcher grace-polls
                    # proc.poll() for --no-signal-grace seconds to catch early
                    # exits (bad gflags, missing .so) before reporting success.
                    env_prefix = ''
                    if custom_env:
                        env_prefix = ' '.join(
                            f'{k}={shlex.quote(str(v))}'
                            for k, v in custom_env.items()) + ' '
                    launcher_parts = [
                        f'cd {shlex.quote(self.remote_work_dir)} &&',
                        env_prefix,
                        'python3', shlex.quote(f'{self.remote_work_dir}/standalone_launcher.py'),
                        '--binary', shlex.quote(f'{self.remote_work_dir}/kvtest'),
                        '--cwd', shlex.quote(self.remote_work_dir),
                        '--log', shlex.quote(f'{self.remote_work_dir}/run.log'),
                        '--pidfile', shlex.quote(f'{self.remote_work_dir}/kvtest.pid'),
                    ]
                    if ld_path:
                        launcher_parts.extend(['--lib-path', shlex.quote(ld_path)])
                    # kvtest client listens on listen_port (HTTP /stop /summary
                    # gateway). Pass --port so the launcher polls TCP connect
                    # instead of falling back to the 2s no-signal grace path
                    # (which prints a misleading "not ready within 2.0s" on
                    # every client that takes >2s to bind).
                    port = node.get('port', self.listen_port)
                    init_wait = (config.get('cuda', {}).get('client_init_wait_seconds', 0)
                                 if config.get('mode') == 'pipeline' else 0)
                    ready_timeout = getattr(self, 'start_timeout', 5) + max(0, init_wait)
                    if port:
                        launcher_parts.extend(['--port', str(port),
                                               '--host', '127.0.0.1',
                                               '--ready-timeout',
                                               str(ready_timeout)])
                    launcher_parts.extend(['--', f'config_{instance_id}.json'])
                    start_cmd = ' '.join(launcher_parts)
                    result = self.run_on(node, start_cmd, check=False,
                                         timeout=max(20, ready_timeout + 10), allow_timeout=True)
                    if result and result.stdout:
                        out = result.stdout.strip()
                        if out:
                            last_line = out.splitlines()[-1].strip()
                            parts = last_line.split()
                            if parts and parts[0].isdigit():
                                pid = parts[0]
                                if len(parts) > 1:
                                    try:
                                        launch_elapsed = float(parts[1])
                                    except ValueError:
                                        pass
                    # Surface launcher stderr (warnings/errors) even when
                    # the process returned 0, so the operator knows if the
                    # launcher reported a not-ready-timeout or early exit.
                    if result and result.stderr and result.stderr.strip():
                        log_info(f'{tag} launcher: {result.stderr.strip()}')
                else:
                    # Fallback: legacy nohup-and-timeout path (used when
                    # launcher upload failed). Kubectl exec / ssh may hang
                    # for the full subprocess timeout because the binary
                    # holds the SPDY pipe; allow_timeout swallows it.
                    env_prefix = (f'LD_LIBRARY_PATH={shlex.quote(ld_path)}:"${{LD_LIBRARY_PATH:-}}" '
                                  if ld_path else '')
                    if custom_env:
                        env_prefix += ' '.join(
                            f'{k}={shlex.quote(str(v))}'
                            for k, v in custom_env.items()) + ' '
                    start_cmd = (
                        f"cd {shlex.quote(self.remote_work_dir)} && "
                        f"{env_prefix}"
                        f"nohup ./kvtest config_{instance_id}.json "
                        f"> run.log 2>&1 </dev/null & "
                        f"echo $!")
                    self.run_on(node, start_cmd, check=False,
                                timeout=10, allow_timeout=True)
                # Use launcher-reported elapsed (Popen → ready, excludes
                # kubectl exec / ssh overhead) when available; fall back to
                # outer measurement for the nohup path.
                start_elapsed = (launch_elapsed if launch_elapsed is not None
                                 else time.monotonic() - t_start)
            except Exception as e:
                log_info(f'  {target} -> FAILED: {e}')
                return False, time.monotonic() - t_start

            # Verify/report process started.
            # Launcher path: PID was already printed to stdout by the
            # launcher (fork + exec; PID survives exec). Report it directly.
            # Fallback path (launcher absent or returned no PID):
            # use pgrep to verify and report.
            if pid:
                log_info(f'{tag} process started (pid={pid})')
            else:
                time.sleep(1)
                verify = self.run_on(
                    node, 'pgrep -x kvtest',
                    check=False)
                if verify.returncode == 0 and verify.stdout.strip():
                    pid = verify.stdout.strip().split('\n')[0]
                    log_info(f'{tag} process started (pid={pid})')
                else:
                    log_info(f'{tag} WARNING: process not found after start, checking log...')
                    log = self.run_on(
                        node, f'cat {self.remote_work_dir}/run.log 2>/dev/null',
                        check=False)
                    if log.stdout.strip():
                        log_info(f'{tag} stdout: {log.stdout.strip()[:500]}')
                    else:
                        log_info(f'{tag} stdout empty — binary may have crashed before any output')

            # Attach procmon (assumes procmon.py was uploaded by install phase;
            # do NOT re-upload per the agreed scope). --background: parent
            # prints PID and exits, kubectl exec / ssh returns immediately.
            if self.enable_procmon and pid:
                procmon_cmd = (
                    f"cd {self.remote_work_dir} && "
                    f"python3 procmon.py --pid {pid} -i 1"
                    f" --output resource_monitor.csv --background")
                procmon_result = self.run_on(node, procmon_cmd, check=False, timeout=10)
                procmon_pid = procmon_result.stdout.strip() if procmon_result.returncode == 0 else ''
                if procmon_pid.isdigit():
                    log_info(f'{tag} procmon started (pid={procmon_pid})')
                else:
                    log_info(f'{tag} WARNING: procmon start may have failed')

            log_info(f'  {target} -> OK')
            return True, start_elapsed
        except Exception as e:
            log_info(f'  {target} -> FAILED: {e}')
            return False, 0.0
        finally:
            if tmp_config is not None:
                os.unlink(tmp_config)

    def deploy_node(self, node, keep_remote_config=False):
        """Legacy single-call install + start (kept for backward compat).

        Equivalent to ``install_node`` followed by ``start_node`` on the same
        node. New callers should use ``do_install`` + ``do_start`` so uploads
        complete cluster-wide before any process starts.
        """
        ok, _ = self.install_node(node)
        if not ok:
            return False, 0.0
        return self.start_node(node, keep_remote_config=keep_remote_config)

    def do_install(self):
        """Install binary + .so + procmon + launcher on all nodes (no start).

        Pre-loads every node so a subsequent ``do_start`` can launch all
        processes near-simultaneously instead of being skewed by upload
        ordering. Critical for 2000+ node clusters where the ~100MB binary
        would otherwise spread start times across minutes.
        """
        if not os.path.isfile(self.binary_path):
            log_info(f'ERROR: binary not found: {self.binary_path}')
            log_info('  Run "build.sh" first to compile and package.')
            sys.exit(1)

        log_info(f'Version: {self.version}')
        log_info(f'\nInstalling on {len(self.nodes)} node(s)...')

        timings = []

        def _install_with_timing(node):
            target = self._exec_target(node)
            try:
                ok, elapsed = self.install_node(node)
            except Exception as e:
                log_info(f'  {target} -> FAILED: {e}')
                ok, elapsed = False, 0.0
            timings.append((target, elapsed, bool(ok)))
            return ok

        results = []
        with ThreadPoolExecutor(max_workers=len(self.nodes) or 1) as pool:
            futures = {pool.submit(_install_with_timing, n): n for n in self.nodes}
            for future in as_completed(futures):
                results.append(future.result())

        ok = sum(1 for r in results if r)
        total = len(results)
        log_info(f'\nInstall result: {ok}/{total} succeeded')
        _print_timings('install', timings)
        return ok == total

    def do_start(self, keep_remote_config=False):
        """Start kvtest on all nodes (assumes install completed).

        Launches all nodes concurrently; with uploads already done in
        ``do_install``, all processes enter Popen within milliseconds of
        each other, giving a true simultaneous-start for cluster-scale
        startup profiling.

        When ``keep_remote_config`` is True, skips per-node config
        generation/upload and reuses the remote ``config_{instance_id}.json``
        left by a prior ``deploy``. Used for rolling upgrades where the
        deploy.json is split into batches but the per-node config (containing
        the full peer topology) must stay intact.
        """
        log_info(f'\nStarting on {len(self.nodes)} node(s)...')

        timings = []

        def _start_with_timing(node):
            target = self._exec_target(node)
            try:
                ok, elapsed = self.start_node(node, keep_remote_config=keep_remote_config)
            except Exception as e:
                log_info(f'  {target} -> FAILED: {e}')
                ok, elapsed = False, 0.0
            timings.append((target, elapsed, bool(ok)))
            return ok

        results = []
        with ThreadPoolExecutor(max_workers=len(self.nodes) or 1) as pool:
            futures = {pool.submit(_start_with_timing, n): n for n in self.nodes}
            for future in as_completed(futures):
                results.append(future.result())

        ok = sum(1 for r in results if r)
        total = len(results)
        log_info(f'\nStart result: {ok}/{total} succeeded')
        _print_timings('start', timings)
        return ok == total

    def do_deploy(self):
        """Full lifecycle: install + start on all nodes.

        Equivalent to ``do_install`` followed by ``do_start``. Unlike calling
        them separately, a partial install failure does NOT abort start:
        nodes whose install failed will also fail at start, but nodes that
        installed successfully are still started so a few bad pods do not
        block the entire cluster.

        Always regenerates and uploads per-node config (calls
        ``do_start(keep_remote_config=False)``) so the started binary runs
        with a config matching the current config_template. Use the ``start``
        subcommand directly (with ``--keep-remote-config``) for rolling
        upgrades where the remote config must be preserved across batches.
        """
        install_ok = self.do_install()
        if not install_ok:
            log_info('\n--- install had failures; starting successful nodes anyway ---')
        else:
            log_info('\n--- install done, starting ---')
        return self.do_start(keep_remote_config=False)

    def do_stop(self, stop_timeout=5):
        """Stop all kvtest instances.

        Single-phase: each node issues one ``standalone_launcher.py stop``
        which sends SIGTERM, polls for exit (up to ``stop_timeout`` seconds),
        and escalates to SIGKILL if needed. Since the kvtest binary's SIGTERM
        handler now mirrors the HTTP /stop RPC (StopNow + RequestStop on the
        main thread), TERM and /stop are equivalent -- no need for the prior
        4-phase HTTP /stop + external pgrep polling + TERM + KILL cascade.

        Falls back to the legacy HTTP /stop + external pgrep polling path
        when no pidfile exists (process started by an older deploy or the
        nohup fallback path).

        ``stop_timeout`` (default 5s, overridable via --stop-timeout) is the
        grace period before SIGKILL escalation; the kvtest binary's teardown
        has a fixed ~5s sleep (pre-drain 3s + post 2s) so 5s is the minimum
        that avoids killing mid-summary-write.
        """
        if not self.nodes:
            log_info('No nodes in deploy config')
            return

        log_info(f'Stopping {len(self.nodes)} instances...')
        timings = []

        def stop_one(node):
            target = self._exec_target(node)
            transport = self._transport(node)
            pidfile = f'{self.remote_work_dir}/kvtest.pid'
            t0 = time.monotonic()

            # Check if pidfile exists (launcher start wrote it). kubectl exec
            # / ssh to test -f; localhost uses os.path.exists.
            if transport == 'localhost':
                has_pidfile = os.path.exists(pidfile)
            else:
                r = self.run_on(node, f'test -f {pidfile}', check=False, timeout=5)
                has_pidfile = r.returncode == 0

            if has_pidfile:
                # Primary path: launcher stop (in-pod TERM → poll → KILL).
                # One round-trip replaces the prior 4-phase cascade.
                launcher = f'{self.remote_work_dir}/standalone_launcher.py'
                cmd = (f'python3 {shlex.quote(launcher)} stop '
                       f'--pidfile {shlex.quote(pidfile)} '
                       f'--binary kvtest --grace {stop_timeout}')
                r = self.run_on(node, cmd, check=False,
                                timeout=stop_timeout + 10 + 30)
                elapsed = time.monotonic() - t0
                ok = r.returncode == 0
                # Parse launcher stdout last line for elapsed detail.
                detail = ''
                if r.stdout:
                    last = r.stdout.strip().splitlines()[-1].strip()
                    parts = last.split()
                    if len(parts) >= 2:
                        try:
                            detail = f' (elapsed={float(parts[1]):.2f}s)'
                        except ValueError:
                            pass
                if ok:
                    log_info(f'  {target} -> stopped{detail}')
                else:
                    log_info(f'  {target} -> FAILED: still running after '
                             f'TERM+KILL{detail}')
                timings.append((target, elapsed, ok))
                return ok

            # Fallback: no pidfile (old version or nohup start). Use the
            # legacy HTTP /stop + external pgrep polling path.
            return self._stop_one_legacy(node, target, t0, timings)

        with ThreadPoolExecutor(max_workers=len(self.nodes) or 1) as pool:
            futures = [pool.submit(stop_one, n) for n in self.nodes]
            for future in as_completed(futures):
                future.result()

        ok = sum(1 for _, _, ok in timings if ok)
        _print_timings('stop', timings)

    def _stop_one_legacy(self, node, target, t0, timings):
        """Legacy stop path for nodes without a pidfile: HTTP /stop + external
        pgrep polling + SIGTERM/SIGKILL escalation. Kept for backward compat
        with processes started before the pidfile feature or via nohup."""
        port = node.get('port', self.listen_port)
        url = f'http://localhost:{port}/stop'
        r = self.run_on(node, f'curl -sf -X POST {url} --max-time 3',
                        check=False, timeout=5)
        if r.returncode != 0:
            r = self.run_on(node, f'wget -qO- --post-data="" --timeout=3 {url}',
                            check=False, timeout=5)
        http_ok = r.returncode == 0

        # Poll for graceful exit (external, since no pidfile for in-pod wait).
        deadline = time.monotonic() + _GRACEFUL_STOP_TIMEOUT
        while time.monotonic() < deadline:
            r = self.run_on(node, 'pgrep -x kvtest 2>/dev/null', check=False)
            if r.returncode != 0:
                break
            time.sleep(_POLL_INTERVAL)

        elapsed = time.monotonic() - t0
        r = self.run_on(node, 'pgrep -x kvtest 2>/dev/null', check=False)
        if r.returncode != 0:
            log_info(f'  {target} -> stopped (legacy, HTTP ok={http_ok})')
            timings.append((target, elapsed, True))
            return True

        # SIGTERM remaining
        self.run_on(node,
                    f"for p in $(pgrep -x kvtest 2>/dev/null); do "
                    f"kill $p 2>/dev/null; done; "
                    f"for p in $(pgrep -x procmon.py 2>/dev/null); do "
                    f"kill $p 2>/dev/null; done",
                    check=False, timeout=10)
        time.sleep(2)

        # SIGKILL if still alive
        r = self.run_on(node, 'pgrep -x kvtest 2>/dev/null', check=False)
        if r.returncode == 0:
            self.run_on(node,
                        f"for p in $(pgrep -x kvtest 2>/dev/null); do "
                        f"kill -9 $p 2>/dev/null; done; "
                        f"for p in $(pgrep -x procmon.py 2>/dev/null); do "
                        f"kill -9 $p 2>/dev/null; done",
                        check=False, timeout=10)
            time.sleep(1)

        r = self.run_on(node, 'pgrep -x kvtest 2>/dev/null', check=False)
        ok = r.returncode != 0
        elapsed = time.monotonic() - t0
        if ok:
            log_info(f'  {target} -> stopped (legacy, after KILL)')
        else:
            log_info(f'  {target} -> FAILED: still running after KILL')
        timings.append((target, elapsed, ok))
        return ok

    def do_clean(self):
        results = []

        def clean_node(node):
            target = self._exec_target(node)
            log_info(f'Cleaning {target}...')
            try:
                # Step 1: Kill processes
                self.run_on(node,
                            "for p in $(pgrep -x kvtest 2>/dev/null); do "
                            "kill $p 2>/dev/null; done; "
                            "for p in $(pgrep -x procmon.py 2>/dev/null); do "
                            "kill $p 2>/dev/null; done",
                            check=False, timeout=15)
                time.sleep(1)

                # Step 2: Force kill remaining
                self.run_on(node,
                            "for p in $(pgrep -x kvtest 2>/dev/null); do "
                            "kill -9 $p 2>/dev/null; done; "
                            "for p in $(pgrep -x procmon.py 2>/dev/null); do "
                            "kill -9 $p 2>/dev/null; done",
                            check=False, timeout=15)
                time.sleep(1)

                # Step 3: Remove directories
                self.run_on(node, f'rm -rf {self.remote_work_dir} /root/.datasystem/logs/',
                            check=False, timeout=15)

                # Verify cleanup
                verify = self.run_on(
                    node, f'ls {self.remote_work_dir} 2>/dev/null',
                    check=False)
                if verify.returncode == 0:
                    log_info(f'  {target} -> WARNING: dir still exists after clean')
                else:
                    log_info(f'  {target} -> OK')
                return True
            except Exception as e:
                log_info(f'  {target} -> FAILED ({e})')
                return False

        with ThreadPoolExecutor(max_workers=len(self.nodes) or 1) as pool:
            futures = [pool.submit(clean_node, n) for n in self.nodes]
            for future in as_completed(futures):
                results.append(future.result())

        ok = sum(1 for r in results if r)
        log_info(f'\nClean result: {ok}/{len(results)}')

    def do_clean_logs(self):
        """Kill processes and remove run-time output, but keep the binary + lib.

        Mirrors ``do_clean`` except the install-phase artifacts (the kvtest
        binary, ``lib/`` .so, ``procmon.py``, ``standalone_launcher.py``)
        are preserved so a re-deploy skips the ~100MB upload on 2000+ node
        clusters. Removes only the per-run products: ``config_*.json``,
        ``run.log``, ``metrics_*/`` output dirs, ``resource_monitor.csv``,
        and the SDK log dir ``/root/.datasystem/logs/``. ``config_*.json`` is
        regenerated by ``start`` so deleting it is safe; ``run.log`` and the
        ``metrics_*/`` dirs are appended/recreated per run, so leaving them
        would stack stale output across runs.
        """
        results = []

        def clean_logs_node(node):
            target = self._exec_target(node)
            log_info(f'Cleaning logs on {target}...')
            try:
                # Step 1: Kill processes (same two-phase kill as do_clean: a
                # graceful TERM first, then -9 for survivors -- a still-running
                # binary would otherwise keep writing to the files we're about
                # to delete and race the rm).
                self.run_on(node,
                            "for p in $(pgrep -x kvtest 2>/dev/null); do "
                            "kill $p 2>/dev/null; done; "
                            "for p in $(pgrep -x procmon.py 2>/dev/null); do "
                            "kill $p 2>/dev/null; done",
                            check=False, timeout=15)
                time.sleep(1)

                # Step 2: Force kill remaining
                self.run_on(node,
                            "for p in $(pgrep -x kvtest 2>/dev/null); do "
                            "kill -9 $p 2>/dev/null; done; "
                            "for p in $(pgrep -x procmon.py 2>/dev/null); do "
                            "kill -9 $p 2>/dev/null; done",
                            check=False, timeout=15)
                time.sleep(1)

                # Step 3: Remove run-time products only (preserve binary +
                # lib/ + procmon.py + standalone_launcher.py installed by
                # install_node). config_*.json is per-instance and regenerated
                # by start_node; run.log + metrics_*/ + resource_monitor.csv
                # are run output that would stack across runs if left.
                self.run_on(
                    node,
                    f'rm -rf {self.remote_work_dir}/config_*.json '
                    f'{self.remote_work_dir}/run.log '
                    f'{self.remote_work_dir}/metrics_* '
                    f'{self.remote_work_dir}/resource_monitor.csv '
                    f'/root/.datasystem/logs/',
                    check=False, timeout=15)

                # Verify install artifacts survived (the whole point of
                # clean-logs vs clean). A missing binary means install never
                # ran or was wiped -- flag it so the operator doesn't hit a
                # confusing "start FAILED: kvtest binary not found" on the
                # next run.
                verify = self.run_on(
                    node, f'test -x {self.remote_work_dir}/kvtest',
                    check=False, timeout=10)
                if verify.returncode == 0:
                    log_info(f'  {target} -> OK (binary preserved)')
                else:
                    log_info(f'  {target} -> WARNING: kvtest binary missing; '
                             f'run install before next start')
                return True
            except Exception as e:
                log_info(f'  {target} -> FAILED ({e})')
                return False

        with ThreadPoolExecutor(max_workers=len(self.nodes) or 1) as pool:
            futures = [pool.submit(clean_logs_node, n) for n in self.nodes]
            for future in as_completed(futures):
                results.append(future.result())

        ok = sum(1 for r in results if r)
        log_info(f'\nClean-logs result: {ok}/{len(results)}')

    def do_collect(self, sdk_log_dir='/root/.datasystem/logs', output_dir='collected',
                   summary_timeout=5, max_workers=None, node_slice=None, filters=None, prefixes=None,
                   instance_ids=None, pod_info=False, archive_options=None, host_selection=None, sdk_only=False):
        """Collect output files and SDK logs from all nodes.

        Single-phase pipeline: each node triggers its own /summary then
        immediately collects its files, all in one bounded ThreadPoolExecutor.
        This eliminates the prior two-phase design where Phase 1 (summary)
        blocked on the slowest node for up to ``summary_timeout`` seconds
        before Phase 2 (collect) could start -- on 2000 nodes one unreachable
        node stalled all 1999 others.

        ``summary_timeout`` (default 5s, overridable via --summary-timeout)
        caps per-node summary retries. The /summary endpoint is synchronous
        (returns HTTP 200 only after ``run_summary.txt`` is written), so
        ``rc=0`` means the file is ready; a dead/unreachable node exhausts
        its own timeout and collects whatever files are available.

        ``max_workers`` bounds the pool (default ``len(nodes)`` = unbounded).
        On large clusters a bounded pool avoids overloading the local fork
        budget and (for kubectl transport) the API server's TLS+impersonation
        connections.

        ``node_slice`` (``(offset, count)`` or ``None``) limits collect to
        a deterministic slice of nodes (sorted by ``host:instance_id``) so
        a 2000-node collect can be manually batched.
        """
        transfer_kwargs = {'archive_options': archive_options} if archive_options is not None else {}
        collect_dir = output_dir
        if not sdk_only:
            copy_case_files(getattr(self, 'case_config_paths', []), collect_dir)
        results = []

        filters = filters or dict(patterns=[], keywords=[], uncompressed_only=False)
        archive_command([], filters)
        if max_workers is not None and max_workers <= 0:
            raise ValueError('--max-workers must be positive')
        nodes = self.nodes
        current_pods = {}
        if host_selection is not None:
            for namespace in {self._namespace(n) for n in nodes if self._transport(n) == 'kubectl'}:
                current_pods.update({(namespace, name): addresses
                                     for name, addresses in read_pod_addresses(namespace).items()})
            def host_ip(node):
                if self._transport(node) == 'kubectl':
                    return current_pods.get((self._namespace(node), node['pod_name']), {}).get('host_ip')
                return node.get('host_ip') or node.get('host')
            nodes = filter_collect_targets(nodes, host_selection, host_ip)
            if not nodes:
                log_error('No clients remain after host IP selection')
                return 1
        if prefixes:
            for prefix in prefixes:
                if not any(node.get('pod_name', '').startswith(prefix) for node in nodes):
                    log_error(f'WARNING: prefix "{prefix}" matched 0 pods')
            nodes = [node for node in nodes if any(node.get('pod_name', '').startswith(prefix)
                                                   for prefix in prefixes)]
            if not nodes:
                log_error('No clients found matching the requested prefixes')
                return 1
        nodes = select_targets(nodes, instance_ids, 'instance_id')
        if node_slice is not None:
            offset, count = node_slice
            if offset < 0:
                offset = 0
            nodes = nodes[offset:offset + count] if count is not None else nodes[offset:]
            if not nodes:
                log_info('No nodes in the requested slice; nothing to collect.')
                return

        for namespace in {self._namespace(n) for n in nodes
                          if self._transport(n) == 'kubectl' and pod_info and host_selection is None}:
            names = [n['pod_name'] for n in nodes if self._transport(n) == 'kubectl'
                     and self._namespace(n) == namespace]
            current_pods.update({(namespace, p['name']): p for p in get_pods(namespace, names)})
        workers = max_workers or (len(nodes) or 1)
        log_info(f'Collecting from {len(nodes)} node(s) with max_workers={workers}...')

        def collect_node(node):
            instance_id = node['instance_id']
            target = self._exec_target(node)
            if pod_info and self._transport(node) == 'kubectl':
                pod = current_pods.get((self._namespace(node), target))
                if pod is None:
                    log_error(f'{target} -> Pod no longer available')
                    return 'fail'
                name = pod_directory(target, pod['ip'], pod.get('host_ip')) + f'__client-{instance_id}'
            else:
                name = f'{target}_{instance_id}'
            local_dir = os.path.join(collect_dir, name)
            log_info(f'Collecting from {target} (instance_id={instance_id})...')

            # Per-node summary trigger (synchronous /summary endpoint).
            # Returns True if rc=0 (file ready), False on timeout.
            port = node.get('port', self.listen_port)
            url = f'http://localhost:{port}/summary'
            deadline = time.monotonic() + (0 if sdk_only or has_filters(filters) else summary_timeout)
            summary_ok = False
            while time.monotonic() < deadline:
                r = self.run_on(node, f'curl -sf -X POST {url} --max-time 3',
                                check=False, timeout=10)
                if r.returncode == 0:
                    summary_ok = True
                    break
                time.sleep(_POLL_INTERVAL)
            if summary_ok:
                log_info(f'  {target} -> summary OK')
            elif not sdk_only and not has_filters(filters):
                log_info(f'  {target} -> summary timeout, collecting available files')

            # Immediately collect this node's files (no global barrier).
            try:
                if sdk_only or has_filters(filters):
                    sources = [['output', self.remote_work_dir, ['*.csv', '*.txt', '*.log', '*.log.*']],
                               ['sdk', sdk_log_dir, ['*.log', '*.log.*', '*.txt']]]
                    if sdk_only:
                        sources = sources[1:]
                    command = archive_command(sources, filters, **transfer_kwargs)
                    transport = self._transport(node)
                    if transport == 'kubectl':
                        command = ['kubectl', 'exec', target, '-n', self._namespace(node), '--', 'sh', '-c', command]
                    elif transport != 'localhost':
                        command = self._build_ssh_cmd(node) + [f'{self._user_for(node)}@{target}', command]
                    count_files = receive_archive(command, local_dir, shell=transport == 'localhost', **transfer_kwargs)
                    log_info(f'  {target} -> {count_files} files collected to {local_dir}/')
                    return 'ok' if count_files else 'empty'
                else:
                    self.collect_files(node, local_dir, **transfer_kwargs)
                    self.collect_sdk_logs(node, local_dir, sdk_log_dir, **transfer_kwargs)
                if not os.path.isdir(local_dir):
                    return 'empty'
                count_files = sum(len(f) for _, _, f in os.walk(local_dir))
                if count_files == 0:
                    log_info(f'  {target} -> 0 files')
                    return 'empty'
                log_info(f'  {target} -> {count_files} files collected to {local_dir}/')
                return 'ok'
            except Exception as e:
                log_info(f'  {target} -> FAILED ({e})')
                return 'fail'

        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = [pool.submit(collect_node, n) for n in nodes]
            for future in as_completed(futures):
                results.append(future.result())

        ok = sum(1 for r in results if r == 'ok')
        empty = sum(1 for r in results if r == 'empty')
        fail = sum(1 for r in results if r == 'fail')
        log_info(f'\nCollect result: {ok} ok / {empty} empty / {fail} fail / {len(results)} total')
        if sdk_only or has_filters(filters) or prefixes or instance_ids or pod_info or archive_options is not None or host_selection is not None:
            return 1 if fail else 0

    def do_run(self, duration):
        """Wait duration then auto stop + collect."""
        def fmt_duration(secs):
            if secs >= 3600:
                return f'{secs // 3600}h {secs % 3600 // 60}m {secs % 60}s'
            if secs >= 60:
                return f'{secs // 60}m {secs % 60}s'
            return f'{secs}s'

        log_info(f'\nRunning for {fmt_duration(duration)}, auto stop + collect after...')
        start = time.time()
        try:
            remaining = duration
            while remaining > 0:
                time.sleep(min(remaining, 60))
                elapsed = int(time.time() - start)
                remaining = duration - elapsed
                if remaining > 0:
                    log_info(f'  [{elapsed}/{duration}s elapsed, {remaining}s remaining]')
        except KeyboardInterrupt:
            elapsed = int(time.time() - start)
            log_info(f'\n  Interrupted after {elapsed}s, stopping early...')

        elapsed = int(time.time() - start)
        log_info(f'\n--- Run finished ({elapsed}s elapsed) ---')
        self.do_stop(stop_timeout=getattr(self, 'stop_timeout', 5))
        self.do_collect(summary_timeout=5)


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

def _parse_pipeline(s):
    return [op.strip() for op in s.split(',') if op.strip()] if s else []


def _parse_manual_nodes(nodes_str):
    """Parse "--nodes h1:p1,h2:p2" into deploy node dicts."""
    if not nodes_str:
        return []
    result = []
    for i, entry in enumerate(nodes_str.split(',')):
        entry = entry.strip()
        if not entry:
            continue
        if ':' in entry:
            host, port = entry.rsplit(':', 1)
            try:
                port = int(port)
            except ValueError:
                log_error(f'ERROR: invalid port in --nodes entry: {entry}')
                sys.exit(1)
        else:
            host = entry
            port = 9000
        result.append({
            'host': host,
            'port': port,
            'instance_id': i,
        })
    return result


def _parse_bool(value):
    """Parse a boolean CLI value: true/false/yes/no/1/0/on/off (case-insensitive)."""
    import argparse
    if isinstance(value, bool):
        return value
    s = str(value).strip().lower()
    if s in ('true', 'yes', '1', 'on'):
        return True
    if s in ('false', 'no', '0', 'off'):
        return False
    raise argparse.ArgumentTypeError(f"invalid boolean value: '{value}' (expected true/false)")


def _build_deploy_config(args, transport, nodes):
    """Assemble the deploy.json payload."""
    deploy = {
        'remote_work_dir': args.remote_work_dir,
        'transport': transport,
        'enable_procmon': False,
        'nodes': nodes,
    }
    if args.remote_sdk_dir:
        deploy['remote_sdk_dir'] = args.remote_sdk_dir
    return deploy


def _build_config(mode, args):
    """Assemble the config.json payload for the given run mode."""
    if mode != 'pipeline' and getattr(args, 'cuda_transfer', False):
        log_error('ERROR: --cuda-transfer supports pipeline mode only')
        sys.exit(1)
    num_threads = args.num_threads if args.num_threads is not None else 4
    cfg = {
        'mode': mode,
        'cluster_name': args.cluster_name or '',
        'num_threads': num_threads,
        'data_sizes': [s.strip() for s in args.data_sizes.split(',')],
        'connect_options': {
            'connect_timeout_ms': 1000,
            'request_timeout_ms': 20,
            'enable_cross_node_connection': True,
            'enable_local_cache': args.enable_local_cache,
            'data_placement_policy': args.data_placement_policy,
            'fast_transport_mem_size': '512MB',
        },
    }
    if mode == 'pipeline':
        cfg['cuda'] = {
            'transfer_enabled': getattr(args, 'cuda_transfer', False),
            'pin': getattr(args, 'cuda_pin', True),
            'device_id': getattr(args, 'cuda_device_id', 0),
            'runtime_library': getattr(args, 'cuda_runtime_library', ''),
            'client_init_wait_seconds': getattr(args, 'cuda_client_init_wait_seconds', 0),
        }
        if cfg['cuda']['client_init_wait_seconds'] < 0:
            log_error('ERROR: --cuda-client-init-wait-seconds must be non-negative')
            sys.exit(1)
        if args.num_total_threads is not None:
            num_total_threads = args.num_total_threads
        elif args.num_threads is not None:
            num_total_threads = num_threads * 2
        else:
            num_total_threads = 16
        if num_total_threads <= num_threads:
            log_error(
                f'ERROR: --num-total-threads ({num_total_threads}) must be greater than '
                f'--num-threads ({num_threads}) for pipeline mode')
            sys.exit(1)
        cfg['num_total_threads'] = num_total_threads
    # Multi-stage QPS: when --stage-target-qps is provided, emit target_qps as
    # an array and stage_duration_seconds so the kvtest binary schedules stage
    # transitions. Otherwise keep the legacy single-int target_qps. Validation
    # of (stage_duration_seconds > 0 when stages set) is done by the binary.
    stage_qps_raw = getattr(args, 'stage_target_qps', '') or ''
    if stage_qps_raw:
        try:
            stage_qps_list = [int(x.strip()) for x in stage_qps_raw.split(',') if x.strip()]
        except ValueError:
            log_error(f'ERROR: --stage-target-qps must be a comma-separated list of integers: {stage_qps_raw}')
            sys.exit(1)
        if not stage_qps_list:
            log_error('ERROR: --stage-target-qps is empty after parsing')
            sys.exit(1)
        cfg['target_qps'] = stage_qps_list
        if args.stage_duration_seconds > 0:
            cfg['stage_duration_seconds'] = args.stage_duration_seconds
    else:
        cfg['target_qps'] = args.target_qps
    # Service discovery address: --jf takes priority (JF discovery),
    # then --coordinator-address (direct), otherwise default to etcd_address.
    if args.jf:
        cfg['jf_address'] = args.jf
        cfg['jf_service'] = args.service or 'kvcache_coordinator'
    elif args.coordinator_address:
        cfg['coordinator_address'] = args.coordinator_address
    else:
        cfg['etcd_address'] = args.etcd_address or '127.0.0.1:2379'

    # CPU / NUMA affinity (all modes)
    if args.cpu_affinity:
        cfg['cpu_affinity'] = args.cpu_affinity
    cfg['random_numa_node'] = bool(args.random_numa_node)
    if args.numa_node is not None:
        cfg['numa_node'] = args.numa_node
        # Specific NUMA node binding takes priority over random selection;
        # clear the random flag so the binary sees one unambiguous signal.
        cfg['random_numa_node'] = False

    # Data verification (pipeline/cache get paths). Only emit the verify block
    # when at least one option differs from the default, to keep generated
    # configs minimal and to preserve the legacy "size, no fail_op" baseline
    # when the user passes no verify flags.
    verify = {}
    if args.verify_level != 'size':
        verify['level'] = args.verify_level
    if args.verify_sample_bytes != '4KB':
        verify['sample_bytes'] = args.verify_sample_bytes
    if args.verify_sample_step != '1MB':
        verify['sample_step'] = args.verify_sample_step
    if args.verify_fail_op:
        verify['fail_op'] = True
    if verify:
        cfg['verify'] = verify

    if mode == 'benchmark':
        cfg['test_mode'] = args.test_mode
        cfg['worker_memory_mb'] = args.worker_memory_mb
        cfg['num_clients'] = args.num_clients
        cfg['set_api'] = args.set_api
        cfg['cleanup_method'] = args.cleanup_method
        if args.total_rounds > 0:
            cfg['total_rounds'] = args.total_rounds
        if args.duration > 0:
            cfg['duration_seconds'] = args.duration
        if args.ttl > 0:
            cfg['set_param'] = {'ttl_second': args.ttl}
        cfg['set_ratio'] = args.set_ratio
        cfg['mixed_key_strategy'] = args.mixed_key_strategy
        cfg['mset_batch_size'] = args.mset_batch_size
        cfg['mget_batch_size'] = args.mget_batch_size
    else:
        # Pipeline / Cache
        writer_pipeline = _parse_pipeline(args.pipeline)
        notify_pipeline = _parse_pipeline(args.notify_pipeline)
        cfg['listen_port'] = 9000
        cfg['role'] = 'writer'
        cfg['pipeline'] = writer_pipeline
        cfg['notify_pipeline'] = notify_pipeline
        cfg['notify_count'] = args.notify_count
        if args.batch_keys_count > 1:
            cfg['batch_keys_count'] = args.batch_keys_count
        if args.ttl > 0:
            cfg['set_param'] = {'ttl_second': args.ttl}
        # Reader-side probabilistic mGet batch sizing (C2 non-blocking).
        # Active when single_prob < 1.0. Requires notify_pipeline to contain
        # mGet for the distribution to actually take effect.
        if args.mget_single_prob < 1.0:
            cfg['mget_size_distribution'] = {
                'single_prob': args.mget_single_prob,
                'min_batch': args.mget_min_batch,
                'max_batch': args.mget_max_batch,
            }
            if args.mget_pending_queue_max > 0:
                cfg['mget_size_distribution']['pending_queue_max'] = args.mget_pending_queue_max
        if mode == 'cache':
            cfg['key_pool_size'] = args.key_pool_size
            if args.target_hit_rate > 0:
                cfg['target_hit_rate'] = args.target_hit_rate
            if args.warmup_timeout != 60:
                cfg['warmup_timeout_seconds'] = args.warmup_timeout
            if args.inference_delay > 0:
                cfg['inference_delay_ms'] = args.inference_delay

    return cfg


def cmd_gen_config(args):
    mode = args.mode

    # --- Validation ---
    if mode == 'benchmark':
        if not args.test_mode:
            log_error('ERROR: --test-mode is required for benchmark mode')
            sys.exit(1)
        if args.worker_memory_mb <= 0:
            log_error('ERROR: --worker-memory-mb is required for benchmark mode')
            sys.exit(1)
    if mode == 'cache':
        if args.key_pool_size <= 0:
            log_error('ERROR: --key-pool-size is required for cache mode')
            sys.exit(1)

    # --- Node discovery ---
    if args.prefixes:
        # Pod discovery via kubectl (all modes)
        pods = get_pods(args.namespace, args.prefixes)
        if not pods:
            log_error(f'No running pods found matching prefixes {args.prefixes} '
                      f'in namespace "{args.namespace}"')
            sys.exit(1)
        # --writer-count defaults to None = all pods are writers. Resolve
        # before validation so the range check sees a concrete number.
        if mode != 'benchmark':
            writer_count = args.writer_count if args.writer_count is not None else len(pods)
            if writer_count < 0 or writer_count > len(pods):
                log_error(f'ERROR: --writer-count ({writer_count}) must be 0..{len(pods)}')
                sys.exit(1)
        else:
            writer_count = None  # benchmark mode ignores writer_count (all writers)

        node_pods = {}
        for i, pod in enumerate(pods):
            node_pods.setdefault(pod['node'], []).append(i)

        writer_indices = set()
        if mode != 'benchmark':
            sorted_nodes = sorted(node_pods.keys())
            pod_queues = {n: list(node_pods[n]) for n in sorted_nodes}
            assigned = 0
            while assigned < writer_count:
                for node in sorted_nodes:
                    if assigned >= writer_count:
                        break
                    if pod_queues[node]:
                        writer_indices.add(pod_queues[node].pop(0))
                        assigned += 1

        writer_pipeline = _parse_pipeline(args.pipeline)
        notify_pipeline = _parse_pipeline(args.notify_pipeline)

        nodes = []
        for i, pod in enumerate(pods):
            is_writer = i in writer_indices if mode != 'benchmark' else True
            node = {
                'pod_name': pod['name'],
                'pod_ip': pod['ip'],
                'namespace': args.namespace,
                'host': pod['node'],
                'host_ip': pod.get('host_ip', ''),  # node InternalIP for HOST_IP env
                'instance_id': i,
                'role': 'writer' if is_writer else 'reader',
                'pipeline': writer_pipeline if is_writer else notify_pipeline,
                'notify_pipeline': notify_pipeline,
            }
            if args.batch_keys_count > 1:
                node['batch_keys_count'] = args.batch_keys_count
            nodes.append(node)
        transport = 'kubectl'
    else:
        # Manual nodes via --nodes or default localhost
        nodes = _parse_manual_nodes(args.nodes)
        if not nodes:
            nodes = [{'host': 'localhost', 'instance_id': 0}]
        transport = 'ssh'

        # Assign writer/reader roles using --writer-count (consistent with kubectl mode)
        writer_indices = set()
        if mode != 'benchmark':
            # --writer-count defaults to None = all nodes are writers.
            writer_count = args.writer_count if args.writer_count is not None else len(nodes)
            if writer_count < 0 or writer_count > len(nodes):
                log_error(f'ERROR: --writer-count ({writer_count}) must be 0..{len(nodes)}')
                sys.exit(1)
            for i in range(min(writer_count, len(nodes))):
                writer_indices.add(i)

            writer_pipeline = _parse_pipeline(args.pipeline)
            notify_pipeline = _parse_pipeline(args.notify_pipeline)
            for i, node in enumerate(nodes):
                is_writer = i in writer_indices
                node['role'] = 'writer' if is_writer else 'reader'
                node['pipeline'] = writer_pipeline if is_writer else notify_pipeline
                node['notify_pipeline'] = notify_pipeline
                if args.batch_keys_count > 1:
                    node['batch_keys_count'] = args.batch_keys_count

    deploy = _build_deploy_config(args, transport, nodes)
    cfg = _build_config(mode, args)

    # --- Write files ---
    os.makedirs(args.output_dir, exist_ok=True)

    deploy_path = os.path.join(args.output_dir, 'deploy.json')
    with open(deploy_path, 'w') as f:
        json.dump(deploy, f, indent=2)
        f.write('\n')
    log_info(f'Generated {deploy_path} ({len(nodes)} nodes, transport={transport})')

    cfg_path = os.path.join(args.output_dir, 'config.json')
    with open(cfg_path, 'w') as f:
        json.dump(cfg, f, indent=2)
        f.write('\n')
    log_info(f'Generated {cfg_path} (mode={mode})')

    for node in nodes:
        target = node.get('pod_name', node.get('host', '?'))
        log_info(f'  {target} -> instance_id={node["instance_id"]}, role={node.get("role", "writer")}')


def _add_gen_config_args(p):
    """Add gen-config arguments to an argparse subparser."""
    p.add_argument('-p', '--prefix', action='append', default=None,
                   dest='prefixes', metavar='PREFIX',
                   help='Pod name prefix to match (repeatable: -p worker-a '
                         '-p worker-b; kubectl discovery). A pod is selected '
                         'if it matches ANY prefix. Omit to use --nodes '
                         'manual mode instead.')
    p.add_argument('-n', '--namespace', default='default',
                   help='k8s namespace (default: default)')
    p.add_argument('-r', '--remote-work-dir',
                   default='/home/user/kvclient_test',
                   help='Remote work directory')
    p.add_argument('-o', '--output-dir', default='config',
                   help='Output directory (default: config)')
    p.add_argument('-e', '--etcd-address',
                   help='Override etcd_address in generated config.json '
                        '(ignored when --coordinator-address is set)')
    p.add_argument('--coordinator-address',
                   help='Use coordinator-backed service discovery instead of etcd: '
                        'sets coordinator_address in config.json and suppresses '
                        'etcd_address. Takes priority over --etcd-address.')
    p.add_argument('--jf',
                   help='Use JF service discovery: sets jf_address in config.json. '
                        'Takes priority over --coordinator-address and --etcd-address.')
    p.add_argument('--service', default='kvcache_coordinator',
                   help='JF service name (default: kvcache_coordinator)')
    p.add_argument('-c', '--cluster-name',
                   help='Set cluster_name in generated config.json')
    p.add_argument('--remote-sdk-dir',
                   help='SDK lib path inside containers')
    p.add_argument('-m', '--mode', default='pipeline',
                   choices=['pipeline', 'cache', 'benchmark'],
                   help='Run mode (default: pipeline)')
    # Node specification (for benchmark without Pod discovery)
    p.add_argument('--nodes',
                   help='Manual node list for deployment, comma-separated host:port pairs '
                        '(e.g. "1.2.3.4:9000,5.6.7.8:9001"). Default: localhost single node')
    # Pipeline / Cache common
    p.add_argument('-w', '--writer-count', type=int, default=None,
                   help='Number of writer instances (default: all pods are writers)')
    p.add_argument('--pipeline', default='setStringView',
                   help='Comma-separated writer pipeline ops (default: setStringView)')
    p.add_argument('--notify-pipeline', default='getBuffer',
                   help='Comma-separated notify pipeline ops (default: getBuffer)')
    p.add_argument('--batch-keys-count', type=int, default=1,
                   help='batch_keys_count for batch ops (default: 1)')
    p.add_argument('--cuda-transfer', action='store_true',
                   help='Enable explicit d2h/h2d/mD2h/mH2d pipeline ops; requires a visible GPU')
    p.add_argument('--cuda-pin', type=_parse_bool, default=True,
                   help='Register Pin callbacks when a GPU is available (true/false, default: true)')
    p.add_argument('--cuda-device-id', type=int, default=0,
                   help='CUDA visible device ordinal, not host GPU index (default: 0)')
    p.add_argument('--cuda-runtime-library', default='',
                   help='Optional absolute libcudart.so path inside the client container')
    p.add_argument('--cuda-client-init-wait-seconds', type=int, default=0,
                   help='Wait after KVClient init before pipeline requests and metrics (default: 0)')
    p.add_argument('--target-qps', type=int, default=100,
                   help='Target QPS, 0=unlimited (default: 100). Use --stage-target-qps '
                        'for multi-stage QPS instead of this single value.')
    p.add_argument('--stage-target-qps', type=str, default='',
                   help='Comma-separated list of target QPS for multi-stage runs, e.g. '
                        '"60,90,120". When set, config.json target_qps is emitted as an array '
                        'and --stage-duration-seconds must be > 0. Takes precedence over '
                        '--target-qps.')
    p.add_argument('--stage-duration-seconds', type=int, default=0,
                   help='Per-stage duration in seconds (default: 0 = disabled). Required '
                        'when --stage-target-qps is set. The kvtest binary schedules stage '
                        'transitions; deploy_client only writes the value to config.json.')
    p.add_argument('--notify-count', type=int, default=10,
                   help='Number of peers to notify per write (default: 10)')
    # Reader-side probabilistic mGet batch sizing (C2 non-blocking variant).
    # When --mget-single-prob < 1.0, each notify-triggered mGet samples a
    # target batch size: with probability --mget-single-prob it is 1,
    # otherwise a uniform draw from [--mget-min-batch, --mget-max-batch].
    # The actual batch is min(target, pending queue depth); no waiting.
    # Requires --notify-pipeline to contain mGet.
    p.add_argument('--mget-single-prob', type=float, default=1.0,
                   help='Probability that a notify-triggered mGet fetches a single key '
                        '(default: 1.0 = disabled, always single-key). Set < 1.0 to enable '
                        'probabilistic batch sizing, e.g. 0.95 for 95%% single / 5%% batch.')
    p.add_argument('--mget-min-batch', type=int, default=2,
                   help='Minimum mGet batch size in the batch branch (default: 2). '
                        'Only used when --mget-single-prob < 1.0.')
    p.add_argument('--mget-max-batch', type=int, default=5,
                   help='Maximum mGet batch size in the batch branch (default: 5). '
                        'Only used when --mget-single-prob < 1.0.')
    p.add_argument('--mget-pending-queue-max', type=int, default=65536,
                   help='Cap on the reader-side pending keys queue (default: 65536, 0 = '
                        'unbounded). Oldest keys are dropped when full. Only used when '
                        '--mget-single-prob < 1.0.')
    p.add_argument('--data-sizes', default='1MB',
                   help='Comma-separated data sizes, e.g. "1MB,512KB" (default: 1MB)')
    p.add_argument('--num-threads', type=int,
                   help='Number of worker threads (default: 4)')
    p.add_argument('--num-total-threads', type=int,
                   help='Total Pipeline read and write threads (default: 16); '
                        'read threads equal this value minus --num-threads')
    p.add_argument('--cleanup-method', default='del',
                   choices=['del', 'ttl'],
                   help='Cleanup method: del (delete keys) or ttl (auto-expire, default: del)')
    # Cache mode
    p.add_argument('--key-pool-size', type=int, default=0,
                   help='Cache mode key pool size (0 = disabled)')
    p.add_argument('--target-hit-rate', type=float, default=0,
                   help='Target cache hit rate 0.01~1.0 (0 = no adjustment)')
    p.add_argument('--warmup-timeout', type=int, default=60,
                   help='Reader warmup timeout in seconds (default: 60)')
    p.add_argument('--inference-delay', type=int, default=0,
                   help='Reader inference delay in ms (default: 0)')
    # Benchmark mode
    p.add_argument('--test-mode',
                   choices=['set_local', 'set_remote', 'get_local',
                            'get_cross_node', 'get_remote_direct', 'get_remote_cross',
                            'mixed_local_set_get', 'mixed_remote_set_get',
                            'mixed_local_set_cross_get', 'mixed_remote_set_remote_cross_get',
                            'mset_local', 'mset_remote',
                            'mget_local', 'mget_cross_node',
                            'mget_remote_direct', 'mget_remote_cross'],
                   help='Benchmark test mode (required for benchmark mode)')
    p.add_argument('--worker-memory-mb', type=int, default=0,
                   help='Worker shared memory in MB (required for benchmark mode)')
    p.add_argument('--num-clients', type=int, default=1,
                   help='Measured KVClient processes per benchmark instance (default: 1)')
    p.add_argument('--set-api', default='string_view',
                   choices=['string_view', 'create_buffer', 'create_buffer_raw'],
                   help='Set API path (default: string_view)')
    p.add_argument('--set-ratio', type=float, default=0.5,
                   help='Set thread ratio for mixed modes (default: 0.5)')
    p.add_argument('--mixed-key-strategy', default='same_keys',
                   choices=['same_keys', 'read_prev', 'independent'],
                   help='Key strategy for mixed modes (default: same_keys)')
    p.add_argument('--mset-batch-size', type=int, default=8,
                   help='Keys per MSet call (default: 8)')
    p.add_argument('--mget-batch-size', type=int, default=8,
                   help='Keys per MGet call (default: 8)')
    p.add_argument('--duration', type=int, default=0,
                   help='Benchmark duration in seconds (0 = infinite)')
    p.add_argument('--total-rounds', type=int, default=0,
                   help='Benchmark total rounds (0 = infinite)')
    p.add_argument('--ttl', type=int, default=0,
                   help='TTL in seconds via set_param.ttl_second (default: 0, no expiry)')
    # Connect options (applies to all modes)
    p.add_argument('--enable-local-cache', type=_parse_bool, default=False,
                   nargs='?', const=True, dest='enable_local_cache',
                   metavar='BOOL',
                   help='Enable SDK client local cache (default: false; bare flag = true). '
                        'Pass false to make Get/MGet query metadata owners through the Transport layer.')
    p.add_argument('--data-placement-policy',
                   choices=['PREFERRED_SAME_NODE', 'REQUIRED_SAME_NODE', 'PREFERRED_META_OWNER'],
                   default='PREFERRED_META_OWNER', dest='data_placement_policy',
                   help='Set/MSet data placement policy (default: PREFERRED_META_OWNER).')
    # CPU / NUMA affinity
    p.add_argument('--cpu-affinity', default='',
                   help='CPU affinity, e.g. "0-7" or "0,2,4,6" (default: auto-detect)')
    numa_group = p.add_mutually_exclusive_group()
    numa_group.add_argument('--numa-node', type=int, default=None,
                            help='NUMA node to bind (default: disabled); requires libnuma. '
                                 'Mutually exclusive with --random-numa-node.')
    numa_group.add_argument('--random-numa-node', action='store_true', default=True,
                            dest='random_numa_node',
                            help='Let kvtest pick a NUMA node at random at startup. '
                                 'Mutually exclusive with --numa-node. Default: on.')
    # Separate (not in the mutually exclusive group) so it can be combined
    # with --numa-node for an explicit "specific node, no randomness" intent,
    # though --numa-node alone already clears random_numa_node in config.
    p.add_argument('--no-random-numa-node', action='store_false',
                   dest='random_numa_node',
                   help='Disable random NUMA node selection (default: on; use this '
                        'to turn it off when you want no NUMA binding at all).')
    # Data verification (pipeline/cache get paths)
    p.add_argument('--verify-level',
                   choices=['off', 'size', 'sample', 'full'], default='size',
                   help='Get data verification level: off/size/sample/full '
                        '(default: size). Pipeline/Cache modes only')
    p.add_argument('--verify-sample-bytes', default='4KB',
                   help='Per-segment sample length for level=sample (default: 4KB). '
                        'Supports KB/MB/GB suffix')
    p.add_argument('--verify-sample-step', default='1MB',
                   help='Distance between sample segment starts for level=sample '
                        '(default: 1MB). Supports KB/MB/GB suffix')
    p.add_argument('--verify-fail-op', action='store_true',
                   help='Verify failure fails the op (counts as Fail). '
                        'Default: only verify_fail counter + warn log')


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description='Deploy kvtest to remote nodes via SSH or kubectl.')
    sub = parser.add_subparsers(dest='command')

    shared = argparse.ArgumentParser(add_help=False)
    shared.add_argument('--kvtest-binary-path',
                        help='Path to kvtest binary (default: adjacent kvtest or output/kvtest; '
                             'overridden by deploy.json kvtest_binary_path)')

    # install
    p = sub.add_parser('install', help='Upload binary + .so + procmon + launcher (no start)',
                       parents=[shared])
    p.add_argument('deploy_json', help='Path to deploy.json')

    # start
    p = sub.add_parser('start', help='Start kvtest on all nodes (assumes install ran)',
                       parents=[shared])
    p.add_argument('--jemalloc_prof_conf',
                   help='Jemalloc MALLOC_CONF; default prof_prefix is <output_dir>/jemalloc/kvtest_<instance_id>')
    p.add_argument('--start-timeout', type=int, default=5,
                   help='Max seconds to wait for a kvtest client to become '
                        'ready after launch (default: 5). The launcher polls '
                        'TCP connect on the HTTP control port.')
    p.add_argument('--keep-remote-config', dest='keep_remote_config',
                   action='store_true', default=True,
                   help='Reuse the remote config_{instance_id}.json left by a prior '
                        'deploy instead of regenerating/uploading (default: True). '
                        'Used for rolling upgrades where deploy.json is split into '
                        'batches but the per-node config (full peer topology) must '
                        'stay intact. Env block and HOST_IP injection are still read '
                        'from the local config_template. Pre-flight fails if the '
                        'remote config is missing (run a full deploy first). Pass '
                        '--no-keep-remote-config to force config regeneration.')
    p.add_argument('--no-keep-remote-config', dest='keep_remote_config',
                   action='store_false',
                   help='Regenerate and upload per-node config (legacy behavior).')
    p.add_argument('deploy_json', help='Path to deploy.json')
    p.add_argument('config_template', nargs='?', default='config/config.json.example',
                   help='Config template (default: config/config.json.example). Required '
                        'for HOST_IP/env injection even with --keep-remote-config.')

    # deploy
    p = sub.add_parser('deploy', help='Install + start (auto stop+collect if duration set)',
                       parents=[shared])
    p.add_argument('deploy_json', help='Path to deploy.json')
    p.add_argument('config_template', nargs='?', default='config/config.json.example',
                   help='Config template (default: config/config.json.example).')
    p.add_argument('--jemalloc_prof_conf',
                   help='Jemalloc MALLOC_CONF for a kvtest built with -b bazel -x on; '
                        'default prof_prefix is <output_dir>/jemalloc/kvtest_<instance_id>')
    p.add_argument('--start-timeout', type=int, default=5,
                   help='Max seconds to wait for a kvtest client to become '
                        'ready after launch (default: 5). The launcher polls '
                        'TCP connect on the HTTP control port.')

    # stop
    p = sub.add_parser('stop', help='Stop all instances (launcher stop: SIGTERM -> SIGKILL)',
                       parents=[shared])
    p.add_argument('deploy_json')
    p.add_argument('config_template', nargs='?', default='config/config.json.example')
    p.add_argument('--stop-timeout', type=int, default=5,
                   help='Max seconds to wait after SIGTERM before '
                        'escalating to SIGKILL (default: 5). The kvtest '
                        'binary teardown has a fixed ~5s sleep.')

    # collect
    p = sub.add_parser('collect', help='Collect output files and SDK logs', parents=[shared])
    add_collect_filters(p)
    p.add_argument('--sdk-only', action='store_true',
                   help='Collect only SDK logs; skip summary, case configs and kvtest outputs')
    p.add_argument('--log-pattern', action='append', dest='file_pattern', metavar='GLOB',
                   help='Alias for --file-pattern; repeatable, quote wildcards. '
                        'Use with --sdk-only to collect only matching SDK logs')
    p.add_argument('-p', '--prefix', action='append', default=None, dest='prefixes', metavar='PREFIX',
                   help='Pod name prefix to match (repeatable: -p client-a -p client-b). '
                        'A pod is selected if it matches ANY prefix.')
    p.add_argument('--instance-ids', nargs='+', default=[], help='Exact client instance IDs to collect')
    p.add_argument('deploy_json')
    p.add_argument('config_template', nargs='?', default='config/config.json.example')
    p.add_argument('-o', '--output', default='collected',
                   help='Local output directory (default: collected)')
    p.add_argument('--sdk-log-dir', default='/root/.datasystem/logs',
                   help='SDK log directory on remote nodes (default: /root/.datasystem/logs)')
    p.add_argument('--summary-timeout', type=int, default=5,
                   help='Per-node summary-trigger timeout in seconds (default: 5). '
                        'The /summary endpoint is synchronous (returns 200 only after '
                        'run_summary.txt is written); each node retries POST /summary '
                        'until rc=0 or this timeout. Lower values start collecting '
                        'sooner when a node is unreachable.')
    p.add_argument('--count', type=int, default=None,
                   help='Limit collect to N nodes (sorted by host:instance_id). '
                        'Pair with --offset to manually batch large clusters.')
    p.add_argument('--offset', type=int, default=0,
                   help='Skip the first N nodes before applying --count (default: 0).')
    p.add_argument('--max-workers', type=int, default=None,
                   help='Max concurrent nodes for collect (default: all). On large '
                        'clusters (500+ nodes) an unbounded pool overloads the local '
                        'fork budget and (for kubectl) the API server.')

    # clean
    p = sub.add_parser('clean', help='Kill processes and remove remote work dirs', parents=[shared])
    p.add_argument('deploy_json')
    p.add_argument('config_template', nargs='?', default='config/config.json.example')

    # clean-logs: same scope as clean but preserves the binary + lib + scripts
    p = sub.add_parser('clean-logs',
                       help='Kill processes and remove run-time output, but keep '
                            'standalone binary + lib + procmon + launcher',
                       parents=[shared])
    p.add_argument('deploy_json')
    p.add_argument('config_template', nargs='?', default='config/config.json.example')

    # gen-config
    p = sub.add_parser('gen-config',
                       help='Generate deploy.json + config.json')
    _add_gen_config_args(p)

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)
    setup_logging()

    if args.command == 'gen-config':
        cmd_gen_config(args)
        return

    # install does not need a config template; pass None so Deployer does
    # not require the file. start/deploy/stop/collect all take an optional
    # config_template positional (default config/config.json.example), so
    # getattr(..., 'config_template', None) returns the actual path or None.
    config_template = getattr(args, 'config_template', None)
    deployer = Deployer(args.deploy_json, config_template)

    default_binary = os.path.join(deployer.base_dir, 'kvtest')
    if not os.path.isfile(default_binary):
        default_binary = os.path.join(deployer.base_dir, 'output', 'kvtest')
    deployer.binary_path = getattr(args, 'kvtest_binary_path', None) or deployer.deploy.get(
        'kvtest_binary_path') or default_binary

    if args.command in ('start', 'deploy'):
        deployer.jemalloc_prof_conf = args.jemalloc_prof_conf
        deployer.start_timeout = getattr(args, 'start_timeout', 5)
        if args.jemalloc_prof_conf is not None:
            try:
                normalize_jemalloc_prof_conf(args.jemalloc_prof_conf, 'logs', 0)
            except ValueError as error:
                parser.error(str(error))
    if args.command == 'install':
        if not deployer.do_install():
            sys.exit(1)
    elif args.command == 'start':
        if not deployer.do_start(keep_remote_config=getattr(args, 'keep_remote_config', False)):
            sys.exit(1)
    elif args.command == 'deploy':
        deployer.do_deploy()
        duration = parse_duration(deployer.deploy.get('duration', '0'))
        if duration > 0:
            deployer.do_run(duration)
    elif args.command == 'stop':
        deployer.do_stop(stop_timeout=getattr(args, 'stop_timeout', 5))
    elif args.command == 'collect':
        node_slice = None
        if getattr(args, 'count', None) is not None or getattr(args, 'offset', 0) > 0:
            node_slice = (getattr(args, 'offset', 0), getattr(args, 'count', None))
        result = deployer.do_collect(
            args.sdk_log_dir, args.output,
            summary_timeout=getattr(args, 'summary_timeout', 5),
            max_workers=getattr(args, 'max_workers', None),
            node_slice=node_slice, filters=filters_from_args(args), sdk_only=args.sdk_only,
            prefixes=args.prefixes, instance_ids=args.instance_ids, pod_info=args.pod_info,
            archive_options=archive_options_from_args(args), host_selection=host_selection_from_args(args))
        if result:
            sys.exit(result)
    elif args.command == 'clean':
        deployer.do_clean()
    elif args.command == 'clean-logs':
        deployer.do_clean_logs()


if __name__ == '__main__':
    main()
