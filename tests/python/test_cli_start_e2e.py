# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""End-to-end dscli start diagnostics tests."""

import os
import stat
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]


@unittest.skipIf(os.name != "posix", "dscli e2e path validation requires POSIX paths")
class CliStartE2eTest(unittest.TestCase):
    """Run the real dscli command entry with a controlled worker process."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()  # pylint: disable=consider-using-with
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.pkg = self.root / "pkg"
        self.ds_dir = self.pkg / "yr" / "datasystem"
        self.home = self.root / "home"
        self.home.mkdir()
        self.fake_worker = self.ds_dir / "datasystem_worker"
        self.install_package_stub()

    def install_package_stub(self):
        self.ds_dir.mkdir(parents=True)
        os.symlink(REPO_ROOT / "cli", self.ds_dir / "cli", target_is_directory=True)
        (self.pkg / "yr" / "__init__.py").write_text("", encoding="utf-8")
        (self.ds_dir / "__init__.py").write_text("__version__ = '0.0.0-test'\n", encoding="utf-8")
        (self.ds_dir / ".commit_id").write_text("__commit_id__ = 'e2e-test'\n", encoding="utf-8")
        (self.ds_dir / "lib").mkdir()
        dist_info = self.pkg / "openyuanrong_datasystem-0.0.0.dist-info"
        dist_info.mkdir()
        (dist_info / "METADATA").write_text(
            "Name: openyuanrong-datasystem\nVersion: 0.0.0-test\n",
            encoding="utf-8",
        )
        for config in ("worker_config.json", "coordinator_config.json"):
            (self.ds_dir / config).write_text("{}\n", encoding="utf-8")
        (self.root / "run_dscli.py").write_text(
            "from yr.datasystem.cli.command import main\n"
            "if __name__ == '__main__':\n"
            "    main()\n",
            encoding="utf-8",
        )

    def run_start(self, worker_source, timeout="1", dscli_args=None):
        self.fake_worker.write_text(worker_source, encoding="utf-8")
        self.fake_worker.chmod(self.fake_worker.stat().st_mode | stat.S_IXUSR)
        env = os.environ.copy()
        env["PYTHONPATH"] = str(self.pkg)
        dscli_args = dscli_args or []
        cmd = [
            sys.executable, str(self.root / "run_dscli.py"), "start", "-t", timeout,
            "-d", str(self.home),
            *dscli_args,
            "-w",
            "--worker_address", "127.0.0.1:31501",
            "--coordinator_address", "127.0.0.1:31511",
            "--ready_check_path", str(self.home / "ready"),
            "--rocksdb_store_dir", str(self.home / "rocksdb"),
            "--unix_domain_socket_dir", str(self.home / "uds"),
            "--log_dir", str(self.home / "logs"),
            "--enable_urma", "false",
        ]
        return subprocess.run(cmd, env=env, text=True, capture_output=True, timeout=6, check=False)

    def test_missing_build_marker_rejects_jemalloc_prof(self):
        result = self.run_start(
            "#!/usr/bin/env python3\n",
            dscli_args=["--jemalloc_prof_conf", "prof_final:true"],
        )
        output = result.stdout + result.stderr

        self.assertNotEqual(result.returncode, 0)
        self.assertIn(
            "Jemalloc profiling is not enabled in the current build", output
        )
        self.assertIn("build.sh -x on", output)

    def test_worker_bind_conflict_reports_address_resource(self):
        result = self.run_start(
            "#!/usr/bin/env python3\n"
            "import sys\n"
            "print('Port 31501 on 127.0.0.1 is already in use', file=sys.stderr)\n"
            "sys.exit(255)\n"
        )
        output = result.stdout + result.stderr

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Worker address 127.0.0.1:31501 is already in use", output)
        self.assertIn("worker_address/bind_address", output)
        self.assertNotIn("metadata store lock", output)
        self.assertNotIn("Start failed: The worker service exited abnormally", output)

    def test_worker_store_lock_conflict_reports_store_resource(self):
        result = self.run_start(
            "#!/usr/bin/env python3\n"
            "import sys\n"
            "print('KV store error. Cannot open the key/value store. "
            "Cannot create/open database: lock file: /tmp/rocksdb/LOCK: "
            "Resource temporarily unavailable', file=sys.stderr)\n"
            "sys.exit(255)\n"
        )
        output = result.stdout + result.stderr

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Worker metadata store lock is unavailable", output)
        self.assertIn(f"rocksdb_store_dir={self.home / 'rocksdb'}", output)
        self.assertIn("worker_address=127.0.0.1:31501", output)
        self.assertIn("startup retries", output)
        self.assertNotIn("Start failed: The worker service exited abnormally", output)

    def test_worker_unrelated_exit_keeps_generic_abnormal_exit(self):
        result = self.run_start(
            "#!/usr/bin/env python3\n"
            "import sys\n"
            "print('Worker runtime error: RPC deadline exceeded', file=sys.stderr)\n"
            "sys.exit(255)\n"
        )
        output = result.stdout + result.stderr

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Worker service exited abnormally with code 255", output)
        self.assertNotIn("metadata store lock", output)
        self.assertNotIn("already in use", output)


if __name__ == "__main__":
    unittest.main()
