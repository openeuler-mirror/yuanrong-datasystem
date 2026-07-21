#!/usr/bin/env python3
import importlib.util
import tempfile
import unittest
from pathlib import Path
from xml.etree import ElementTree as ET


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "ensure_clion_compdb_project.py"


def load_module():
    spec = importlib.util.spec_from_file_location("ensure_clion_compdb_project", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class EnsureClionCompdbProjectTest(unittest.TestCase):
    def test_sanitizes_cmake_state_and_keeps_compdb_project_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            project = Path(tmp) / "worker-self-healing-main-20260716"
            idea = project / ".idea"
            idea.mkdir(parents=True)
            (idea / "workspace.xml").write_text(
                """<?xml version="1.0" encoding="UTF-8"?>
<project version="4">
  <component name="CMakePresetLoader">{}</component>
  <component name="CMakeProjectFlavorService">
    <option name="flavorId" value="CMakePlainProjectFlavor" />
  </component>
  <component name="CMakeRunConfigurationManager">
    <generated />
  </component>
  <component name="CMakeSettings">
    <configurations>
      <configuration PROFILE_NAME="Debug" ENABLED="true" CONFIG_NAME="Debug" />
    </configurations>
  </component>
  <component name="RunManager">
    <configuration default="true" type="CMakeRunConfiguration" factoryName="Application">
      <method v="2" />
    </configuration>
  </component>
  <component name="VCPKGProject">
    <isAutomaticReloadCMake value="true" />
  </component>
  <component name="CompDBLocalSettings">
    <option name="projectSyncType">
      <map>
        <entry key="$PROJECT_DIR$" value="RE_IMPORT" />
      </map>
    </option>
  </component>
</project>
""",
                encoding="utf-8",
            )

            module = load_module()
            module.ensure_clion_compdb_project(project)

            workspace = (idea / "workspace.xml").read_text(encoding="utf-8")
            self.assertNotIn("CMakeSettings", workspace)
            self.assertNotIn("CMakeRunConfiguration", workspace)
            self.assertNotIn("VCPKGProject", workspace)
            self.assertIn("CompDBLocalSettings", workspace)

            misc = (idea / "misc.xml").read_text(encoding="utf-8")
            self.assertIn('component name="CompDBSettings"', misc)
            self.assertIn('component name="CompDBWorkspace"', misc)

            modules = (idea / "modules.xml").read_text(encoding="utf-8")
            iml = idea / "worker-self-healing-main-20260716.iml"
            self.assertIn("worker-self-healing-main-20260716.iml", modules)
            self.assertIn('external.system.id="CompDB"', iml.read_text(encoding="utf-8"))

            for path in (idea / "workspace.xml", idea / "misc.xml", idea / "modules.xml", iml):
                ET.parse(path)

    def test_remote_build_script_repairs_idea_before_and_after_index_generation(self):
        script = (ROOT / "scripts" / "clion_remote_build.sh").read_text(encoding="utf-8")
        first_repair = script.index("python3 scripts/ensure_clion_compdb_project.py")
        sync = script.index("==> Syncing worktree")
        symlink = script.index('ln -sfn "${BUILD_DIR}/compile_commands.json" compile_commands.json')
        second_repair = script.index("python3 scripts/ensure_clion_compdb_project.py", first_repair + 1)
        ready = script.index('echo "==> Ready:')

        self.assertLess(first_repair, sync)
        self.assertLess(symlink, second_repair)
        self.assertLess(second_repair, ready)

    def test_excludes_generated_and_bazel_paths_from_clion_module(self):
        with tempfile.TemporaryDirectory() as tmp:
            project = Path(tmp) / "worker-self-healing-main-20260716"

            module = load_module()
            module.ensure_clion_compdb_project(project)

            iml = (project / ".idea" / "worker-self-healing-main-20260716.iml").read_text(encoding="utf-8")
            for excluded in (
                ".bazel-cache",
                ".clion-remote",
                ".codegraph",
                "bazel-bin",
                "bazel-out",
                "bazel-testlogs",
                "bazel-worker-self-healing-main-20260716",
                "cmake-build-debug",
            ):
                self.assertIn(f'<excludeFolder url="file://$MODULE_DIR$/../{excluded}" />', iml)


if __name__ == "__main__":
    unittest.main()
