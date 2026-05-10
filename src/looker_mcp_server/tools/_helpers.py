"""Private helpers shared across tool groups.

Intentionally scoped to small utilities that multiple tool modules need.
Anything that grows beyond one-liners should move to its own module.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Annotated, Any
from urllib.parse import quote

from ..client import LookerSession


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


def _validate_branch_args(branch: str | None, project_id: str | None) -> None:
    """Branch swap requires a non-empty project ID and branch name.

    Looker scopes branches per project, and ``LookerSession.use_branch``
    needs the path segment to issue ``GET/PUT /projects/{id}/git_branch``.
    Empty/whitespace branch strings would otherwise reach Looker as
    ``{"name": ""}`` and surface as an opaque 400 — much worse signal
    than a self-describing ``ValueError`` here. Raised as ``ValueError``
    so ``format_api_error`` formats it cleanly.
    """
    if branch is not None and not branch.strip():
        raise ValueError(
            "branch=… must be a non-empty branch name; got an empty or whitespace-only value."
        )
    if branch is not None and not project_id:
        raise ValueError(
            "branch=… requires project_id=…; pass the LookML project ID that "
            "owns the branch you want to atomically swap to."
        )


@asynccontextmanager
async def _maybe_use_branch(
    session: LookerSession, project_id: str | None, branch: str | None
) -> AsyncGenerator[None, None]:
    """Wrap the body in ``session.use_branch`` only when ``branch`` is set.

    Tools that take an optional ``branch`` argument don't want to nest
    yet another ``async with`` for the no-branch case, but they also need
    the atomic save+restore semantics when a branch IS set. This helper
    keeps the call site flat in both modes.

    This is *dispatch* logic, not validation: it decides whether a swap
    is requested at all. Validity of the requested swap (non-empty
    branch, non-empty project_id) is enforced upstream by
    ``_validate_branch_args``. Adding empty-string guards here would
    silently skip the swap on invalid input rather than fail loud, which
    is the wrong failure mode — it would mask upstream bugs that forgot
    to call the validator.
    """
    if branch is None or project_id is None:
        yield
        return
    async with session.use_branch(project_id, branch):
        yield
