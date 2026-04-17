"""``WWW-Authenticate`` challenge builders for 401 and 403 responses.

Per the MCP 2025-11-25 authorization spec (PRM Discovery; Scope Challenge
Handling), a resource server MUST emit a ``WWW-Authenticate`` header on
401 pointing at its Protected Resource Metadata (RFC 9728 §5.1), and
SHOULD emit a scope challenge on 403 per RFC 6750 §3.1.

RFC 7235 §4.1 makes the ``realm`` parameter mandatory; RFC 7230 §3.2.6
defines the ``quoted-string`` grammar that requires backslash-escaping of
``\\`` and ``"`` inside the value.
"""

from __future__ import annotations


def escape_quoted_string(value: str) -> str:
    r"""Escape ``value`` for inclusion in an HTTP ``quoted-string``.

    The two characters that need escaping inside a ``quoted-string`` per
    RFC 7230 §3.2.6 are ``\`` and ``"``. Other characters (including
    whitespace) are permitted verbatim.

    Intentionally does not attempt to strip control characters — the
    caller is expected to pass reasonable URIs / realm labels.
    """
    if not any(c in value for c in ("\\", '"')):
        return value
    out: list[str] = []
    for c in value:
        if c in ("\\", '"'):
            out.append("\\")
        out.append(c)
    return "".join(out)


def invalid_token_challenge(*, realm: str, resource_metadata_url: str) -> str:
    """Build the ``WWW-Authenticate`` value for a 401 Unauthorized.

    ``realm=`` is mandatory per RFC 7235 §4.1. ``resource_metadata=`` is
    the absolute URI of the Protected Resource Metadata document, per
    MCP 2025-11-25 PRM-Discovery + RFC 9728 §5.1.
    """
    if not realm:
        raise ValueError("realm must not be empty")
    if not resource_metadata_url:
        raise ValueError("resource_metadata_url must not be empty")
    return (
        f'Bearer realm="{escape_quoted_string(realm)}", '
        f'resource_metadata="{escape_quoted_string(resource_metadata_url)}"'
    )


def insufficient_scope_challenge(
    *,
    realm: str,
    required_scopes: list[str] | None = None,
    description: str = "the access token lacks a required scope",
) -> str:
    """Build the ``WWW-Authenticate`` value for a 403 Forbidden.

    MCP 2025-11-25 §Scope Challenge Handling + RFC 6750 §3.1. ``scope=``
    is omitted when ``required_scopes`` is empty/None.
    """
    if not realm:
        raise ValueError("realm must not be empty")
    parts = [
        f'realm="{escape_quoted_string(realm)}"',
        'error="insufficient_scope"',
        f'error_description="{escape_quoted_string(description)}"',
    ]
    if required_scopes:
        parts.append(f'scope="{escape_quoted_string(" ".join(required_scopes))}"')
    return "Bearer " + ", ".join(parts)
