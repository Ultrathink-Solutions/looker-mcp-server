"""Private helpers shared across tool groups.

Intentionally scoped to small utilities that multiple tool modules need.
Anything that grows beyond one-liners should move to its own module.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator, Callable, Collection
from contextlib import asynccontextmanager
from typing import Annotated, Any, TypeVar
from urllib.parse import quote

from fastmcp import FastMCP
from fastmcp.tools import Tool
from fastmcp.tools.tool_transform import ArgTransform

from ..client import LookerSession

_F = TypeVar("_F", bound=Callable[..., Any])


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
    "Looker user to run this call as, given as a numeric user ID or an email "
    "address, for example '42' or 'ci-bot@example.com'. Use it to work in "
    "another user's dev workspace or to run as a dedicated service user. "
    "Default: the current identity."
)
ActAsUser = Annotated[str | None, ACT_AS_USER_DESCRIPTION]
ACT_AS_USER_ARG = "act_as_user"

# The query tools' developer arguments: the dev-workspace switch and the
# atomic branch swap (``project_id`` exists only to name the branch's
# project). They are offered only when the deployment opts in with
# ``LOOKER_QUERY_DEV_PARAMS``.
DEV_ARGS = ("dev_mode", "branch", "project_id")
DevMode = Annotated[
    bool,
    "Set true to use the LookML checked out in the dev workspace instead of "
    "production, for example to test unmerged changes. Default false; setting "
    "branch turns it on.",
]
DevBranch = Annotated[
    str | None,
    "Git branch to check out in the dev workspace for this call only, for example "
    "'feature-x'. The workspace's previous branch is restored afterwards, even on "
    "failure. Requires project_id and turns on dev_mode. Default: no branch change.",
]
DevProjectId = Annotated[
    str | None,
    "LookML project that owns branch, for example 'ecommerce'. Required when branch "
    "is set. Default: none.",
]


def tool_with_hidden_args(
    server: FastMCP,
    *,
    description: str,
    hidden_args: Collection[str] = (),
) -> Callable[[_F], _F]:
    """Register a tool like ``@server.tool``, leaving ``hidden_args`` out of it.

    Lets one function body serve every deployment shape: a tool whose
    optional arguments only some deployments can use is defined once, with
    all of them, and the arguments a deployment cannot use are removed from
    the registered schema here. FastMCP's argument transform does the
    removal, so a hidden argument is absent from ``tools/list`` and a call
    that sends one anyway is rejected as an unexpected argument instead of
    being silently ignored. The function body receives the parameter's
    default. Every hidden argument must therefore have a default.

    Names in ``hidden_args`` that the function does not take are ignored,
    so callers can pass one shared set to tools with different signatures.
    """

    def decorator(fn: _F) -> _F:
        tool: Tool = Tool.from_function(fn, description=description)
        offered = tool.parameters.get("properties") or {}
        transforms = {name: ArgTransform(hide=True) for name in hidden_args if name in offered}
        if transforms:
            tool = Tool.from_tool(tool, transform_args=transforms)
        server.add_tool(tool)
        return fn

    return decorator


# Per-call hidden-content toggle shared across discovery tools (explore,
# query). LookML ``hidden: yes`` is a curation signal — model authors hide
# fields and explores they don't want surfaced in the field picker — so
# discovery tools honor it by default and expose this escape hatch.
INCLUDE_HIDDEN_DESCRIPTION = "Set true to also return items marked hidden in LookML. Default false."
IncludeHidden = Annotated[bool, INCLUDE_HIDDEN_DESCRIPTION]


def _filter_hidden(items: list[dict[str, Any]], include_hidden: bool) -> list[dict[str, Any]]:
    """Drop items whose ``hidden`` flag is truthy unless ``include_hidden``.

    Items without a ``hidden`` key are always kept — not every Looker
    payload carries the flag, and absence means "not hidden".
    """
    if include_hidden:
        return items
    return [item for item in items if not item.get("hidden")]


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
