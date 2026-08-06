import argparse
import importlib.util
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock


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


class SensitiveContentValidationTest(unittest.TestCase):
    def test_rejects_sensitive_pr_body_without_echoing_value(self) -> None:
        body = "验证日志在 <remote-log-dir>/run.log，服务为 192.0.2.10:2222"
        with self.assertRaises(SystemExit) as exc:
            MODULE.validate_no_sensitive_content({"PR body": body})
        message = str(exc.exception)
        self.assertIn("Sensitive information is not allowed", message)
        self.assertIn("PR body", message)
        self.assertNotIn("<remote-log-dir>", message)
        self.assertNotIn("192.0.2.10", message)

    def test_build_payload_rejects_sensitive_title_and_squash_commit_message(self) -> None:
        args = argparse.Namespace(
            owner="openeuler",
            repo="another-repo",
            title="fix: update token=abc123",
            head="feature-branch",
            base="master",
            body="safe body",
            body_file=None,
            milestone_number=None,
            labels=None,
            issue=None,
            assignees=None,
            testers=None,
            prune_source_branch=False,
            draft=False,
            squash=True,
            squash_commit_message="fix: remove -----BEGIN OPENSSH PRIVATE KEY-----",
            fork_path=None,
            api_base="https://api.gitcode.com/api/v5",
            token=None,
            token_file=None,
            body_template_file=None,
            timeout=30,
            check_conflicts=True,
        )
        with self.assertRaises(SystemExit) as exc:
            MODULE.build_payload(args)
        message = str(exc.exception)
        self.assertIn("Sensitive information is not allowed", message)
        self.assertIn("PR title", message)
        self.assertIn("squash commit message", message)
        self.assertNotIn("abc123", message)


class RetestCommentTest(unittest.TestCase):
    @mock.patch.object(MODULE, "build_payload", return_value={"title": "test"})
    @mock.patch.object(MODULE, "load_token", return_value="test-token")
    @mock.patch.object(MODULE, "request_json")
    def test_create_pr_posts_retest_comment_after_creation(
        self,
        request_json: mock.Mock,
        _load_token: mock.Mock,
        _build_payload: mock.Mock,
    ) -> None:
        request_json.side_effect = [
            {"number": 42, "html_url": "https://gitcode.com/example/repo/pull/42"},
            {"id": 7, "body": "/retest"},
        ]
        args = argparse.Namespace(
            owner="example",
            repo="repo",
            api_base="https://api.gitcode.com/api/v5",
            token=None,
            token_file=None,
            timeout=30,
            check_conflicts=False,
        )

        MODULE.create_pr(args)

        self.assertEqual(request_json.call_count, 2)
        request_json.assert_any_call(
            "POST",
            "https://api.gitcode.com/api/v5/repos/example/repo/issues/42/comments?access_token=test-token",
            {"body": "/retest"},
            30,
        )

    @mock.patch.object(MODULE, "request_json")
    def test_retest_comment_falls_back_to_pull_comment_endpoint_on_404(
        self,
        request_json: mock.Mock,
    ) -> None:
        request_json.side_effect = [
            SystemExit("GitCode API failed: HTTP 404"),
            {"id": 8, "body": "/retest"},
        ]
        args = argparse.Namespace(
            owner="example",
            repo="repo",
            api_base="https://api.gitcode.com/api/v5",
            timeout=30,
        )

        result = MODULE.post_retest_comment(
            args,
            "test-token",
            {"number": 42, "html_url": "https://gitcode.com/example/repo/pull/42"},
        )

        self.assertEqual(result["body"], "/retest")
        self.assertEqual(request_json.call_count, 2)
        request_json.assert_called_with(
            "POST",
            "https://api.gitcode.com/api/v5/repos/example/repo/pulls/42/comments?access_token=test-token",
            {"body": "/retest"},
            30,
        )

    @mock.patch.object(MODULE, "build_payload", return_value={"title": "test"})
    @mock.patch.object(MODULE, "load_token", return_value="test-token")
    @mock.patch.object(MODULE, "request_json")
    def test_create_pr_skips_retest_when_branch_policy_allows_it(
        self,
        request_json: mock.Mock,
        _load_token: mock.Mock,
        _build_payload: mock.Mock,
    ) -> None:
        request_json.return_value = {
            "number": 42,
            "html_url": "https://gitcode.com/example/repo/pull/42",
        }
        args = argparse.Namespace(
            owner="example",
            repo="repo",
            api_base="https://api.gitcode.com/api/v5",
            token=None,
            token_file=None,
            timeout=30,
            check_conflicts=False,
            skip_retest=True,
        )

        MODULE.create_pr(args)

        self.assertEqual(request_json.call_count, 1)


class RetestPolicyTest(unittest.TestCase):
    def test_skips_retest_for_docs_and_repo_context_only_changes(self) -> None:
        policy = getattr(MODULE, "retest_skip_reason", None)
        self.assertIsNotNone(policy, "create_pr.py must classify documentation-only PRs")

        reason = policy(
            "master",
            ["docs/guide.md", ".repo_context/index.md", ".repo_context/modules/overview.md"],
        )

        self.assertEqual(reason, "docs_or_repo_context_only")

    def test_requires_retest_when_any_source_path_changes(self) -> None:
        policy = getattr(MODULE, "retest_skip_reason", None)
        self.assertIsNotNone(policy, "create_pr.py must classify mixed PRs")

        reason = policy("master", ["docs/guide.md", "src/datasystem/client/client.cc"])

        self.assertIsNone(reason)

    def test_skips_retest_for_doc_pages_base_even_with_source_changes(self) -> None:
        policy = getattr(MODULE, "retest_skip_reason", None)
        self.assertIsNotNone(policy, "create_pr.py must classify doc_pages PRs")

        reason = policy("doc_pages", ["src/datasystem/client/client.cc"])

        self.assertEqual(reason, "base_is_doc_pages")


class GitBranchPreparationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        root = Path(self.tempdir.name)
        self.worktree = root / "worktree"
        self.remote = root / "fork.git"
        subprocess.run(["git", "init", "--bare", str(self.remote)], check=True, capture_output=True, text=True)
        subprocess.run(
            ["git", "init", "--initial-branch=master", str(self.worktree)],
            check=True,
            capture_output=True,
            text=True,
        )
        self.git("config", "user.name", "Skill Test")
        self.git("config", "user.email", "skill-test@example.com")
        (self.worktree / "data.txt").write_text("base\n", encoding="utf-8")
        self.git("add", "data.txt")
        self.git("commit", "-m", "chore: base")
        self.git("switch", "-c", "feature")
        self.git("remote", "add", "fork", str(self.remote))

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def git(self, *args: str) -> str:
        result = subprocess.run(
            ["git", "-C", str(self.worktree), *args],
            check=True,
            capture_output=True,
            text=True,
        )
        return result.stdout.strip()

    def commit_change(self, content: str, message: str) -> None:
        (self.worktree / "data.txt").write_text(content, encoding="utf-8")
        self.git("add", "data.txt")
        self.git("commit", "-m", message)

    def prepare_args(self, *, push_remote: str = "fork", message: str | None = None) -> argparse.Namespace:
        return argparse.Namespace(
            owner="openeuler",
            repo="yuanrong-datasystem",
            head="example:feature",
            base="master",
            git_worktree=self.worktree,
            base_ref="master",
            push_remote=push_remote,
            local_squash_message=message,
        )

    def test_squashes_multiple_commits_to_one_and_updates_existing_remote_branch(self) -> None:
        self.commit_change("first\n", "fix: first change")
        self.commit_change("second\n", "fix: second change")
        old_head = self.git("rev-parse", "HEAD")
        old_tree = self.git("rev-parse", "HEAD^{tree}")
        self.git("push", "--set-upstream", "fork", "HEAD:refs/heads/feature")
        prepare = getattr(MODULE, "prepare_branch_for_pr", None)
        self.assertIsNotNone(prepare, "create_pr.py must prepare and squash the source branch")

        result = prepare(self.prepare_args(message="fix: combine feature changes"))

        new_head = self.git("rev-parse", "HEAD")
        remote_head = subprocess.run(
            ["git", "--git-dir", str(self.remote), "rev-parse", "refs/heads/feature"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        self.assertNotEqual(new_head, old_head)
        self.assertEqual(self.git("rev-list", "--count", "master..HEAD"), "1")
        self.assertEqual(self.git("rev-parse", "HEAD^{tree}"), old_tree)
        self.assertEqual(remote_head, new_head)
        self.assertEqual(self.git("rev-parse", result["backup_ref"]), old_head)
        self.assertTrue(result["squashed"])
        self.assertEqual(result["commit_count_before"], 2)
        self.assertEqual(result["commit_count_after"], 1)

    def test_keeps_single_commit_unchanged_and_pushes_it(self) -> None:
        self.commit_change("single\n", "fix: single change")
        old_head = self.git("rev-parse", "HEAD")
        prepare = getattr(MODULE, "prepare_branch_for_pr", None)
        self.assertIsNotNone(prepare, "create_pr.py must prepare the source branch")

        result = prepare(self.prepare_args())

        remote_head = subprocess.run(
            ["git", "--git-dir", str(self.remote), "rev-parse", "refs/heads/feature"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        self.assertEqual(self.git("rev-parse", "HEAD"), old_head)
        self.assertEqual(remote_head, old_head)
        self.assertFalse(result["squashed"])
        self.assertIsNone(result["backup_ref"])

    def test_refuses_to_push_source_branch_to_upstream_repository(self) -> None:
        self.commit_change("single\n", "fix: single change")
        self.git("remote", "add", "upstream", "git@gitcode.com:openeuler/yuanrong-datasystem.git")
        prepare = getattr(MODULE, "prepare_branch_for_pr", None)
        self.assertIsNotNone(prepare, "create_pr.py must prepare the source branch")

        with self.assertRaises(SystemExit) as exc:
            prepare(self.prepare_args(push_remote="upstream"))

        self.assertIn("upstream", str(exc.exception).lower())

    def test_refuses_base_ref_that_does_not_match_pr_base_branch(self) -> None:
        self.commit_change("single\n", "fix: single change")
        self.git("branch", "other-base", "master")
        args = self.prepare_args()
        args.base_ref = "other-base"
        prepare = getattr(MODULE, "prepare_branch_for_pr", None)
        self.assertIsNotNone(prepare, "create_pr.py must prepare the source branch")

        with self.assertRaises(SystemExit) as exc:
            prepare(args)

        self.assertIn("does not match", str(exc.exception))

    def test_accepts_remote_tracking_ref_for_base_branch_with_slashes(self) -> None:
        self.commit_change("single\n", "fix: single change")
        self.git("branch", "release/0.9.1", "master")
        args = self.prepare_args()
        args.base = "release/0.9.1"
        args.base_ref = "upstream/release/0.9.1"
        self.git("update-ref", "refs/remotes/upstream/release/0.9.1", "release/0.9.1")

        result = MODULE.prepare_branch_for_pr(args)

        self.assertEqual(result["commit_count_after"], 1)

    def test_rename_from_source_to_docs_still_requires_retest(self) -> None:
        (self.worktree / "docs").mkdir()
        self.git("mv", "data.txt", "docs/data.txt")
        self.git("commit", "-m", "docs: move data file")

        result = MODULE.prepare_branch_for_pr(self.prepare_args())

        self.assertIn("changed_files", result)
        self.assertEqual(set(result["changed_files"]), {"data.txt", "docs/data.txt"})
        self.assertIsNone(result["retest_skip_reason"])


if __name__ == "__main__":
    unittest.main()
