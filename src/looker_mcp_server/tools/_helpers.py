"""Private helpers shared across tool groups.

Intentionally scoped to small utilities that multiple tool modules need.
Anything that grows beyond one-liners should move to its own module.
"""

from __future__ import annotations

from typing import Any
from urllib.parse import quote


def _set_if(body: dict[str, Any], key: str, value: Any) -> None:
    """Add ``key`` to ``body`` only when ``value`` is not ``None``.

    Keeps tool signatures flat (optional ``| None`` args) without forwarding
    explicit ``None`` values that Looker would interpret as "clear this field".
    """
    if value is not None:
        body[key] = value


def _path_seg(value: str | int) -> str:
    """Percent-encode a single URL path segment.

    Used at every path-parameter interpolation site so reserved characters
    (space, slash, etc.) in user- or caller-supplied IDs cannot misroute
    requests.  Defaults to ``safe=''`` so even ``/`` is encoded — critical
    for values that would otherwise be interpreted as sub-paths.
    """
    return quote(str(value), safe="")
