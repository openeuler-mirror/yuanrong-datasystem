import datetime as dt
import importlib.util
from unittest import mock
import tempfile
import unittest
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "refresh_online_docs.py"
SPEC = importlib.util.spec_from_file_location("ds_refresh_docs_script", SCRIPT_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)

CREATE_PR_PATH = Path(__file__).resolve().parents[2] / "ds-create-pr" / "scripts" / "create_pr.py"
CREATE_PR_SPEC = importlib.util.spec_from_file_location("ds_create_pr_script", CREATE_PR_PATH)
CREATE_PR_MODULE = importlib.util.module_from_spec(CREATE_PR_SPEC)
assert CREATE_PR_SPEC.loader is not None
CREATE_PR_SPEC.loader.exec_module(CREATE_PR_MODULE)


def subprocess_completed_process(returncode: int, stdout: str) -> object:
    class Completed:
        def __init__(self) -> None:
            self.returncode = returncode
            self.stdout = stdout

    return Completed()


class RefreshOnlineDocsTest(unittest.TestCase):
    def test_parse_gitcode_remote_accepts_https_and_ssh_fork_urls(self) -> None:
        self.assertEqual(
            MODULE.parse_gitcode_remote("https://gitcode.com/yaohaolin/yuanrong-datasystem.git"),
            ("yaohaolin", "yuanrong-datasystem"),
        )
        self.assertEqual(
            MODULE.parse_gitcode_remote("git@gitcode.com:yaohaolin/yuanrong-datasystem.git"),
            ("yaohaolin", "yuanrong-datasystem"),
        )

    def test_resolve_pr_source_remote_rejects_upstream_remote(self) -> None:
        completed = subprocess_completed_process(
            returncode=0,
            stdout="git@gitcode.com:openeuler/yuanrong-datasystem.git\n",
        )
        with mock.patch.object(MODULE.subprocess, "run", return_value=completed):
            with self.assertRaises(SystemExit) as exc:
                MODULE.resolve_pr_source_remote(Path("/tmp/repo"), "origin")
        self.assertIn("points to upstream", str(exc.exception))

    def test_create_pr_command_uses_fork_owner_branch_and_body_file(self) -> None:
        pr_body = Path("/tmp/pr-body.md")
        command = MODULE.create_pr_command(
            "doc_pages",
            "docs-refresh-branch",
            "docs: refresh zh-cn latest pages",
            pr_body,
            "yaohaolin",
            "yuanrong-datasystem",
        )
        self.assertEqual(command[0], MODULE.sys.executable)
        self.assertIn("--head", command)
        self.assertIn("yaohaolin:docs-refresh-branch", command)
        self.assertIn("--fork-path", command)
        self.assertIn("yaohaolin/yuanrong-datasystem", command)
        self.assertIn(str(pr_body), command)

    def test_validate_upstream_url_accepts_https_and_ssh(self) -> None:
        self.assertEqual(
            MODULE.validate_upstream_url("https://gitcode.com/openeuler/yuanrong-datasystem.git"),
            "https://gitcode.com/openeuler/yuanrong-datasystem.git",
        )
        self.assertEqual(
            MODULE.validate_upstream_url("git@gitcode.com:openeuler/yuanrong-datasystem.git"),
            "git@gitcode.com:openeuler/yuanrong-datasystem.git",
        )

    def test_validate_upstream_url_rejects_non_upstream_url(self) -> None:
        with self.assertRaises(SystemExit) as exc:
            MODULE.validate_upstream_url("https://gitcode.com/someone/yuanrong-datasystem.git")
        self.assertIn("Online docs refresh must build from the latest upstream master", str(exc.exception))

    def test_write_pr_body_uses_repository_template_sections(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            pr_body = Path(tmpdir) / "pr-body.md"
            MODULE.write_pr_body(
                pr_body,
                "docs-refresh-branch",
                "abc123",
                "https://gitcode.com/openeuler/yuanrong-datasystem.git",
                "deadbeef12345678",
                "2026-04-18T12:34:56+08:00",
                True,
            )
            content = pr_body.read_text(encoding="utf-8")
        self.assertIn("**这是什么类型的PR？**", content)
        self.assertIn("/kind docs", content)
        self.assertIn("**Self-checklist**", content)
        self.assertIn("docs-refresh-branch", content)
        self.assertIn("abc123", content)
        self.assertIn("deadbeef12345678", content)
        self.assertIn("2026-04-18T12:34:56+08:00", content)

    def test_write_pr_body_passes_create_pr_template_validation(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            pr_body = Path(tmpdir) / "pr-body.md"
            MODULE.write_pr_body(
                pr_body,
                "docs-refresh-branch",
                "abc123",
                "git@gitcode.com:openeuler/yuanrong-datasystem.git",
                "deadbeef12345678",
                "2026-04-18T12:34:56+08:00",
                False,
            )
            content = pr_body.read_text(encoding="utf-8")
        self.assertEqual(
            CREATE_PR_MODULE.validate_pr_body(content, CREATE_PR_MODULE.DEFAULT_PR_TEMPLATE_FILE),
            content,
        )

    def test_generated_at_text_returns_isoformat_with_timezone(self) -> None:
        generated_at = MODULE.generated_at_text()
        parsed = dt.datetime.fromisoformat(generated_at)
        self.assertIsNotNone(parsed.tzinfo)

    def test_prepare_source_worktree_uses_detached_worktree_for_new_path(self) -> None:
        repo_root = Path("/tmp/repo")
        worktree = Path("/tmp/source-worktree")
        with (
            mock.patch.object(MODULE, "fetch_branch") as fetch_branch,
            mock.patch.object(MODULE, "run") as run,
            mock.patch.object(Path, "exists", return_value=False),
        ):
            result = MODULE.prepare_source_worktree(repo_root, worktree, "master", "openeuler")
        self.assertEqual(result, worktree)
        fetch_branch.assert_called_once_with(repo_root, "openeuler", "master")
        run.assert_called_once_with(
            ["git", "worktree", "add", "--detach", str(worktree), "openeuler/master"],
            repo_root,
        )


if __name__ == "__main__":
    unittest.main()
