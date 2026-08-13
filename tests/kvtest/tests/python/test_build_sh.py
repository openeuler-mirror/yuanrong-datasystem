#!/usr/bin/env python3
"""Behavior tests for the standalone kvtest build entrypoint."""

import os
import pathlib
import shutil
import stat
import subprocess
import tempfile
import unittest


KVTEST_DIR = pathlib.Path(__file__).resolve().parents[2]


class TestKvtestBuildScript(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self._tmp.name)
        self.script_dir = self.root / "tests" / "kvtest"
        self.script_dir.mkdir(parents=True)
        shutil.copy2(KVTEST_DIR / "build.sh", self.script_dir / "build.sh")
        shutil.copy2(KVTEST_DIR / "VERSION", self.script_dir / "VERSION")

        self.bin_dir = self.root / "fake-bin"
        self.bin_dir.mkdir()
        self.bazel_log = self.root / "bazel.log"
        self._write_executable(
            "bazel",
            """#!/bin/sh
printf '%s\n' "$*" >> "$KVTEST_BAZEL_LOG"
if [ "$1" = "build" ]; then
    mkdir -p "$PWD/bazel-bin/tests/kvtest"
    printf '#!/bin/sh\n' > "$PWD/bazel-bin/tests/kvtest/kvtest"
    chmod +x "$PWD/bazel-bin/tests/kvtest/kvtest"
fi
""",
        )
        self._write_executable("gcc", "#!/bin/sh\nexit 1\n")
        self._write_executable("ldconfig", "#!/bin/sh\nexit 1\n")
        self._write_executable("make", "#!/bin/sh\nexit 0\n")

    def tearDown(self):
        self._tmp.cleanup()

    def _write_executable(self, name, content):
        path = self.bin_dir / name
        path.write_text(content, encoding="utf-8")
        path.chmod(path.stat().st_mode | stat.S_IXUSR)

    def _run(self, *args):
        env = os.environ.copy()
        env["PATH"] = f"{self.bin_dir}:{env['PATH']}"
        env["KVTEST_BAZEL_LOG"] = str(self.bazel_log)
        env["JOBS"] = "2"
        return subprocess.run(
            ["bash", str(self.script_dir / "build.sh"), *args],
            cwd=self.script_dir,
            env=env,
            text=True,
            capture_output=True,
            check=False,
        )

    def _bazel_commands(self):
        if not self.bazel_log.exists():
            return []
        return self.bazel_log.read_text(encoding="utf-8").splitlines()

    def test_urma_option_controls_bazel_config(self):
        cases = [
            (["-b", "bazel", "-c", "-M", "on"], True),
            (["-b", "bazel", "-M", "off"], False),
            (["-b", "bazel"], False),
        ]
        for args, expect_urma in cases:
            with self.subTest(args=args):
                self.bazel_log.unlink(missing_ok=True)
                result = self._run(*args)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                build_commands = [cmd for cmd in self._bazel_commands() if cmd.startswith("build ")]
                self.assertEqual(len(build_commands), 1, self._bazel_commands())
                self.assertEqual("--config=urma" in build_commands[0], expect_urma)

    def test_rejects_invalid_urma_option(self):
        result = self._run("-b", "bazel", "-M", "invalid")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("choose from on or off", result.stdout + result.stderr)

    def test_rejects_missing_urma_option(self):
        result = self._run("-b", "bazel", "-M")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("-M requires on or off", result.stdout + result.stderr)

    def test_rejects_urma_option_for_cmake_sdk_build(self):
        result = self._run("-b", "cmake", "-M", "on")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("-M on is supported only with -b bazel", result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main()
