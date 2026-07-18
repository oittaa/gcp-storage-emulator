"""GCS objects.list matchGlob pattern matching.

Syntax:
https://cloud.google.com/storage/docs/json_api/v1/objects/list#list-object-glob
"""

from __future__ import annotations

import re
from functools import lru_cache
from typing import List


def _split_brace_options(inner: str) -> List[str]:
    options: List[str] = []
    current: List[str] = []
    depth = 0
    for ch in inner:
        if ch == "{":
            depth += 1
            current.append(ch)
        elif ch == "}":
            depth -= 1
            current.append(ch)
        elif ch == "," and depth == 0:
            options.append("".join(current))
            current = []
        else:
            current.append(ch)
    options.append("".join(current))
    return options


def expand_braces(pattern: str) -> List[str]:
    """Expand ``{a,b}`` alternatives (GCS forbids ``/`` and ``**`` inside braces)."""
    results: List[str] = []

    def helper(s: str) -> None:
        start = s.find("{")
        if start == -1:
            results.append(s)
            return
        depth = 0
        end = -1
        for i in range(start, len(s)):
            if s[i] == "{":
                depth += 1
            elif s[i] == "}":
                depth -= 1
                if depth == 0:
                    end = i
                    break
        if end == -1:
            results.append(s)
            return
        before = s[:start]
        after = s[end + 1 :]
        for option in _split_brace_options(s[start + 1 : end]):
            helper(before + option + after)

    helper(pattern)
    return results


def _consume_character_class(pattern: str, start: int) -> tuple:
    """Parse a ``[...]`` class starting at *start*. Returns (regex, next_index)."""
    n = len(pattern)
    j = start + 1
    if j < n and pattern[j] in ("!", "^"):
        j += 1
    if j < n and pattern[j] == "]":
        j += 1
    while j < n and pattern[j] != "]":
        j += 1
    if j >= n:
        return re.escape(pattern[start]), start + 1
    cls = pattern[start : j + 1]
    if cls.startswith("[!"):
        cls = "[^" + cls[2:]
    return cls, j + 1


def _consume_star(pattern: str, start: int) -> tuple:
    """Parse ``*``, ``**``, or ``**/`` at *start*. Returns (regex, next_index)."""
    n = len(pattern)
    if start + 1 < n and pattern[start + 1] == "*":
        if start + 2 < n and pattern[start + 2] == "/":
            # **/ — zero or more path segments ending in /
            return "(?:.*/)?", start + 3
        return ".*", start + 2
    return "[^/]*", start + 1


def _glob_to_regex(pattern: str) -> re.Pattern:
    i = 0
    n = len(pattern)
    out: List[str] = ["^"]
    while i < n:
        ch = pattern[i]
        if ch == "\\" and i + 1 < n:
            out.append(re.escape(pattern[i + 1]))
            i += 2
        elif ch == "*":
            piece, i = _consume_star(pattern, i)
            out.append(piece)
        elif ch == "?":
            out.append("[^/]")
            i += 1
        elif ch == "[":
            piece, i = _consume_character_class(pattern, i)
            out.append(piece)
        else:
            out.append(re.escape(ch))
            i += 1
    out.append("$")
    return re.compile("".join(out))


@lru_cache(maxsize=256)
def _compiled_patterns(pattern: str) -> tuple:
    return tuple(_glob_to_regex(part) for part in expand_braces(pattern))


def gcs_glob_match(pattern: str, name: str) -> bool:
    """Return True if *name* matches the GCS *matchGlob* pattern."""
    if not pattern:
        return False
    return any(regex.match(name) is not None for regex in _compiled_patterns(pattern))
