"""Private helpers shared across tool groups.

Intentionally scoped to small utilities that multiple tool modules need.
Anything that grows beyond one-liners should move to its own module.
"""

from __future__ import annotations

from typing import Any


def _set_if(body: dict[str, Any], key: str, value: Any) -> None:
    """Add ``key`` to ``body`` only when ``value`` is not ``None``.

    Keeps tool signatures flat (optional ``| None`` args) without forwarding
    explicit ``None`` values that Looker would interpret as "clear this field".
    """
    if value is not None:
        body[key] = value
