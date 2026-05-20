import json
import tempfile
import unittest
from pathlib import Path

import sys


HOOK_DIR = Path(__file__).resolve().parents[1] / "hooks"
sys.path.insert(0, str(HOOK_DIR))

import context_survival  # noqa: E402


class ContextSurvivalTest(unittest.TestCase):
    def test_routes_worker_object_cache_paths_to_small_context_set(self):
        docs = context_survival.route_context_docs(
            [
                "src/datasystem/worker/object_cache/service/worker_oc_service_batch_get_impl.cpp",
                "include/datasystem/utils/status.h",
            ]
        )

        self.assertIn(".repo_context/modules/runtime/worker-runtime.md", docs)
        self.assertIn(".repo_context/modules/client/client-sdk.md", docs)
        self.assertIn(".repo_context/modules/quality/tests-and-reproduction.md", docs)
        self.assertLessEqual(len(docs), context_survival.MAX_CONTEXT_DOCS)

    def test_blocks_destructive_shell_commands(self):
        decision = context_survival.evaluate_bash_command("git reset --hard HEAD~1")

        self.assertTrue(decision.blocked)
        self.assertIn("destructive", decision.reason.lower())

    def test_renders_pretool_context_without_blocking_normal_commands(self):
        payload = {
            "hook_event_name": "PreToolUse",
            "tool_name": "Bash",
            "tool_input": {
                "command": "rtk rg -n \"BatchGet\" src/datasystem/worker/object_cache",
            },
            "cwd": "/tmp/repo",
        }

        output = context_survival.handle_pre_tool_use(payload)

        self.assertNotIn("permissionDecision", output["hookSpecificOutput"])
        self.assertIn("additionalContext", output["hookSpecificOutput"])
        self.assertIn("Relevant repo context", output["hookSpecificOutput"]["additionalContext"])

    def test_routes_natural_language_prompt_keywords(self):
        payload = {
            "hook_event_name": "UserPromptSubmit",
            "prompt": "修复 worker batch get 的超时问题",
            "cwd": "/tmp/repo",
        }

        output = context_survival.handle_user_prompt_submit(payload)

        context = output["hookSpecificOutput"]["additionalContext"]
        self.assertIn(".repo_context/modules/runtime/worker-runtime.md", context)

    def test_working_state_snapshot_is_source_backed_and_bounded(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / ".repo_context/modules/runtime").mkdir(parents=True)
            (root / ".codex/context").mkdir(parents=True)
            state = context_survival.WorkingState(
                repo_root=root,
                event_name="Stop",
                touched_paths=["src/datasystem/common/kvstore/etcd/etcd_keep_alive.cpp"],
                recent_tools=["Bash: rtk rg -n etcd src/datasystem/common/kvstore"],
                last_assistant_message="Implemented the first pass and still need validation.",
            )

            rendered = context_survival.render_working_state(state)

        self.assertIn("# Codex Working State", rendered)
        self.assertIn("src/datasystem/common/kvstore/etcd/etcd_keep_alive.cpp", rendered)
        self.assertIn(".repo_context/modules/runtime/etcd-metadata/README.md", rendered)
        self.assertLessEqual(len(rendered.splitlines()), context_survival.MAX_WORKING_STATE_LINES)

    def test_session_start_emits_json_additional_context(self):
        payload = {
            "hook_event_name": "SessionStart",
            "source": "startup",
            "cwd": "/tmp/repo",
            "model": "gpt-5.5",
        }

        output = context_survival.handle_session_start(payload)

        json.dumps(output)
        specific = output["hookSpecificOutput"]
        self.assertEqual(specific["hookEventName"], "SessionStart")
        self.assertIn("Context survival", specific["additionalContext"])

    def test_post_compact_uses_common_output_shape(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / ".repo_context").mkdir()
            (root / ".codex/context").mkdir(parents=True)
            (root / ".codex/context/working-state.md").write_text("# state\n", encoding="utf-8")
            payload = {
                "hook_event_name": "PostCompact",
                "cwd": str(root),
            }

            output = context_survival.handle_post_compact(payload)

        self.assertTrue(output["continue"])
        self.assertIn("systemMessage", output)
        self.assertNotIn("hookSpecificOutput", output)


if __name__ == "__main__":
    unittest.main()
