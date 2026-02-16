from __future__ import annotations

from typing import Optional

TEST_SOURCE_KEYWORDS = (
    "test",
    "mock",
    "demo",
    "manual",
    "sample",
)

TEST_REASON_KEYWORDS = (
    "测试",
    "演示",
    "mock",
    "demo",
    "sample",
    "杭州橙色",
)


def looks_like_test_source(source: Optional[str]) -> bool:
    if not source:
        return False
    text = str(source).strip().lower()
    return any(keyword in text for keyword in TEST_SOURCE_KEYWORDS)


def looks_like_test_reason(reason: Optional[str]) -> bool:
    if not reason:
        return False
    text = str(reason).strip().lower()
    return any(keyword in text for keyword in TEST_REASON_KEYWORDS)


def is_test_warning(*, source: Optional[str], reason: Optional[str]) -> bool:
    return looks_like_test_source(source) or looks_like_test_reason(reason)

