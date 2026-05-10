"""Private helpers shared across tool groups.

Intentionally scoped to small utilities that multiple tool modules need.
Anything that grows beyond one-liners should move to its own module.
"""

from __future__ import annotations

from typing import Annotated, Any
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


# Per-call admin impersonation argument shared across tool groups (git,
# query, modeling). The MCP forwards capability — Looker enforces whether
# the configured admin credentials may impersonate (HTTP 403 if not).
# Email values are resolved via Looker's user-search API; numeric values
# are used directly. ``ArgumentSudoIdentityProvider`` in :mod:`..identity`
# notices this argument and rewrites the resolved identity to a sudo
# session targeting ``act_as_user``.
ACT_AS_USER_DESCRIPTION = (
    "Optional Looker user ID or email to impersonate for this call. "
    "Use to operate on another user's dev workspace (Looker dev mode is "
    "per-user-isolated) or to run as a dedicated CI service user. Requires "
    "sudo capability on the configured admin credentials. When omitted, the "
    "call uses the configured or gateway-provided identity."
)
ActAsUser = Annotated[str | None, ACT_AS_USER_DESCRIPTION]
