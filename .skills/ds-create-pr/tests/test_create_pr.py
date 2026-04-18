import argparse
import importlib.util
import os
import tempfile
import unittest
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "create_pr.py"
SPEC = importlib.util.spec_from_file_location("ds_create_pr_script", SCRIPT_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


class LoadTokenTest(unittest.TestCase):
    def setUp(self) -> None:
        self.original_env = {name: os.environ.get(name) for name in MODULE.TOKEN_ENV_NAMES}
        for name in MODULE.TOKEN_ENV_NAMES:
            os.environ.pop(name, None)

    def tearDown(self) -> None:
        for name in MODULE.TOKEN_ENV_NAMES:
            os.environ.pop(name, None)
        for name, value in self.original_env.items():
            if value is not None:
                os.environ[name] = value

    def test_prefers_non_empty_explicit_token(self) -> None:
        token = MODULE.load_token("  abc123  ", None)
        self.assertEqual(token, "abc123")

    def test_rejects_blank_env_token_with_clear_message(self) -> None:
        os.environ["GITCODE_TOKEN"] = "   "
        with tempfile.TemporaryDirectory() as tmpdir:
            token_file = Path(tmpdir) / "gitcode_token"
            with self.assertRaises(SystemExit) as exc:
                MODULE.load_token(None, token_file)
        self.assertIn("GITCODE_TOKEN is configured but empty", str(exc.exception))

    def test_rejects_missing_explicit_token_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            token_file = Path(tmpdir) / "missing_token"
            with self.assertRaises(SystemExit) as exc:
                MODULE.load_token(None, token_file)
        self.assertIn("GitCode token file not found", str(exc.exception))

    def test_rejects_empty_token_file_with_clear_message(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            token_file = Path(tmpdir) / "gitcode_token"
            token_file.write_text("  \n", encoding="utf-8")
            with self.assertRaises(SystemExit) as exc:
                MODULE.load_token(None, token_file)
        self.assertIn("GitCode token file", str(exc.exception))
        self.assertIn("is configured but empty", str(exc.exception))


class PullRequestBodyTemplateTest(unittest.TestCase):
    def test_validate_pr_body_accepts_complete_template_sections(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            template_file = Path(tmpdir) / "template.md"
            template_file.write_text(
                "\n".join(
                    [
                        "**这是什么类型的PR？**",
                        "",
                        "**这个PR是做什么的/我们为什么需要它**",
                        "",
                        "**此PR修复了哪些问题**:",
                        "",
                        "**PR对程序接口进行了哪些修改？**",
                        "",
                        "**Self-checklist**:",
                    ]
                ),
                encoding="utf-8",
            )
            body = "\n".join(
                [
                    "**这是什么类型的PR？**",
                    "/kind docs",
                    "**这个PR是做什么的/我们为什么需要它**",
                    "refresh docs",
                    "**此PR修复了哪些问题**:",
                    "Fixes #",
                    "**PR对程序接口进行了哪些修改？**",
                    "none",
                    "**Self-checklist**:",
                    "- [x] 验证",
                ]
            )
            self.assertEqual(MODULE.validate_pr_body(body, template_file), body)

    def test_validate_pr_body_rejects_missing_required_sections(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            template_file = Path(tmpdir) / "template.md"
            template_file.write_text("**这是什么类型的PR？**\n\n**Self-checklist**:\n", encoding="utf-8")
            with self.assertRaises(SystemExit) as exc:
                MODULE.validate_pr_body("**这是什么类型的PR？**\n/kind docs\n", template_file)
        self.assertIn("PR body does not follow the required template", str(exc.exception))

    def test_build_payload_requires_template_body_for_default_repo(self) -> None:
        args = argparse.Namespace(
            owner="openeuler",
            repo="yuanrong-datasystem",
            title="docs: refresh zh-cn latest pages",
            head="docs-refresh-branch",
            base="doc_pages",
            body=None,
            body_file=None,
            milestone_number=None,
            labels=None,
            issue=None,
            assignees=None,
            testers=None,
            prune_source_branch=False,
            draft=False,
            squash=False,
            squash_commit_message=None,
            fork_path=None,
            api_base="https://api.gitcode.com/api/v5",
            token=None,
            token_file=None,
            body_template_file=MODULE.DEFAULT_PR_TEMPLATE_FILE,
            timeout=30,
            check_conflicts=True,
        )
        with self.assertRaises(SystemExit) as exc:
            MODULE.build_payload(args)
        self.assertIn("PR body is required for this repository", str(exc.exception))


if __name__ == "__main__":
    unittest.main()
