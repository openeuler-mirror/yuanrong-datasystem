#!/usr/bin/env python3
from __future__ import annotations

import json
import urllib.error
import urllib.parse
import urllib.request
from typing import Any


class GitCodeClient:
    def __init__(self, owner: str, repo: str, token: str, base_urls: list[str]):
        self.owner = owner
        self.repo = repo
        self.token = token
        self.base_urls = [url.rstrip("/") for url in base_urls if url]

    def _headers(self) -> dict[str, str]:
        return {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.token}",
            "PRIVATE-TOKEN": self.token,
            "User-Agent": "yuanrong-pr-review-skill/0.1",
        }

    def _request(self, method: str, path: str, data: dict[str, Any] | None = None) -> Any:
        body = None if data is None else json.dumps(data).encode("utf-8")
        last_error: Exception | None = None

        for base_url in self.base_urls:
            url = f"{base_url}{path}"
            request = urllib.request.Request(url, data=body, headers=self._headers(), method=method)
            try:
                with urllib.request.urlopen(request, timeout=30) as response:
                    payload = response.read()
                    if not payload:
                        return None
                    return json.loads(payload.decode("utf-8"))
            except urllib.error.HTTPError as exc:
                last_error = exc
                if exc.code not in {401, 403, 404, 422, 500, 502, 503}:
                    raise
            except Exception as exc:  # pragma: no cover - network dependent
                last_error = exc

        if last_error:
            raise last_error
        raise RuntimeError(f"Request failed for {path}")

    def _paginate(self, path: str) -> list[dict[str, Any]]:
        page = 1
        items: list[dict[str, Any]] = []

        while True:
            query = urllib.parse.urlencode({"page": page, "per_page": 100})
            payload = self._request("GET", f"{path}?{query}")
            if not payload:
                break
            if not isinstance(payload, list):
                raise RuntimeError(f"Expected list response for {path}, got {type(payload)!r}")
            items.extend(payload)
            if len(payload) < 100:
                break
            page += 1

        return items

    def get_pull(self, number: int) -> dict[str, Any]:
        return self._request("GET", f"/repos/{self.owner}/{self.repo}/pulls/{number}")

    def list_pull_files(self, number: int) -> list[dict[str, Any]]:
        return self._paginate(f"/repos/{self.owner}/{self.repo}/pulls/{number}/files")

    def list_pull_comments(self, number: int) -> list[dict[str, Any]]:
        return self._paginate(f"/repos/{self.owner}/{self.repo}/pulls/{number}/comments")

    def post_pull_comment(
        self,
        number: int,
        body: str,
        path: str,
        absolute_line: int,
        need_to_resolve: bool,
    ) -> Any:
        payload = {
            "body": body,
            "path": path,
            "position": absolute_line,
            "need_to_resolve": need_to_resolve,
        }
        return self._request("POST", f"/repos/{self.owner}/{self.repo}/pulls/{number}/comments", payload)

    def post_general_comment(self, number: int, body: str) -> Any:
        issue_path = f"/repos/{self.owner}/{self.repo}/issues/{number}/comments"
        try:
            return self._request("POST", issue_path, {"body": body})
        except Exception:
            # Some deployments treat PR comments as issue comments, while others accept body-only PR comments.
            return self._request("POST", f"/repos/{self.owner}/{self.repo}/pulls/{number}/comments", {"body": body})
