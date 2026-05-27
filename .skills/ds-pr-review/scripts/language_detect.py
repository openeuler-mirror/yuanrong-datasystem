#!/usr/bin/env python3
from __future__ import annotations

import re


CODE_BLOCK_RE = re.compile(r"```.*?```", re.DOTALL)
INLINE_CODE_RE = re.compile(r"`[^`]*`")
LINK_RE = re.compile(r"https?://\S+")
ISSUE_RE = re.compile(r"[#@][A-Za-z0-9_-]+")
CHINESE_RE = re.compile(r"[\u4e00-\u9fff]")


def _strip_noise(text: str) -> str:
    cleaned = CODE_BLOCK_RE.sub(" ", text or "")
    cleaned = INLINE_CODE_RE.sub(" ", cleaned)
    cleaned = LINK_RE.sub(" ", cleaned)
    cleaned = ISSUE_RE.sub(" ", cleaned)
    return re.sub(r"\s+", " ", cleaned).strip()


def detect_review_language(text: str) -> str:
    cleaned = _strip_noise(text)
    if not cleaned:
        return "zh"
    if CHINESE_RE.search(cleaned):
        return "zh"
    return "en"
