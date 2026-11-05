from __future__ import annotations

import re
from collections import Counter
from datetime import datetime, timedelta
from typing import Callable, TypeVar

T = TypeVar("T")

SIMILARITY_THRESHOLD = 0.7
TIME_WINDOW = timedelta(hours=6)
MIN_GRAMS = 4
MIN_BODY_CHARS = 12

_BRACKET_PREFIX = re.compile(r"^\s*(\[[^\]]{1,20}\]|【[^】]{1,20}】|<[^>]{1,20}>)\s*")
_TRANSLATE = str.maketrans({"北": "북", "南": "남", "美": "미", "中": "중", "日": "일", "韓": "한", "與": "여", "野": "야"})


def title_grams(title: str) -> frozenset[str]:
    full = title.translate(_TRANSLATE).lower()
    body = full
    while True:
        stripped = _BRACKET_PREFIX.sub("", body, count=1)
        if stripped == body:
            break
        body = stripped
    body = re.sub(r"[^\w가-힣]+", "", body)
    # Series headers like "【오늘의 법조】 11월 5일" differ only in the bracket.
    text = body if len(body) >= MIN_BODY_CHARS else re.sub(r"[^\w가-힣]+", "", full)
    return frozenset(text[index:index + 2] for index in range(len(text) - 1))


def title_similarity(left: str, right: str) -> float:
    a, b = title_grams(left), title_grams(right)
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def group_similar(
    items: list[T],
    *,
    title: Callable[[T], str],
    published: Callable[[T], datetime],
    groupable: Callable[[T], bool],
) -> list[list[T]]:
    """Group items with near-identical titles published close together.

    Order is preserved: each group sits where its first item was, and the
    first item leads the group. Items that are not groupable stay alone.
    """
    groups: list[list[T]] = []
    member_grams: list[frozenset[str]] = []
    member_times: list[datetime] = []
    member_group: list[int] = []
    postings: dict[str, list[int]] = {}
    for item in items:
        if not groupable(item):
            groups.append([item])
            continue
        grams = title_grams(title(item))
        when = published(item)
        target = None
        if len(grams) >= MIN_GRAMS:
            shared = Counter(member for gram in grams for member in postings.get(gram, ()))
            best = 0.0
            for member, common in shared.items():
                score = common / (len(grams) + len(member_grams[member]) - common)
                if score >= SIMILARITY_THRESHOLD and score > best and abs(when - member_times[member]) <= TIME_WINDOW:
                    best, target = score, member_group[member]
        if target is None:
            target = len(groups)
            groups.append([item])
        else:
            groups[target].append(item)
        if len(grams) >= MIN_GRAMS:
            member = len(member_grams)
            member_grams.append(grams)
            member_times.append(when)
            member_group.append(target)
            for gram in grams:
                postings.setdefault(gram, []).append(member)
    return groups
