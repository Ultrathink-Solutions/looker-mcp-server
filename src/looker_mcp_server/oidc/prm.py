"""Protected Resource Metadata document (RFC 9728 §2).

The MCP 2025-11-25 authorization spec requires resource servers to expose
PRM at ``/.well-known/oauth-protected-resource`` (and a path-suffix
variant per RFC 9728 §3). MCP clients like Claude Code use this document
to auto-discover which authorization server to route OAuth flows through.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel


class ProtectedResourceMetadata(BaseModel):
    """Typed model of the PRM response body (RFC 9728 §2)."""

    resource: str
    authorization_servers: list[str]
    bearer_methods_supported: list[str] = ["header"]
    scopes_supported: list[str] | None = None
    resource_signing_alg_values_supported: list[str] | None = None
    resource_name: str | None = None
    resource_documentation: str | None = None


def build_prm_document(
    *,
    resource_uri: str,
    authorization_server_issuer_url: str,
    scopes_supported: list[str] | None = None,
    advertise_asymmetric_algs: bool = True,
    resource_name: str | None = None,
    resource_documentation: str | None = None,
) -> dict[str, Any]:
    """Produce the PRM document as a plain dict (JSON-serializable).

    Args:
        resource_uri: The canonical URI of the resource server (audience
            value). MUST be https and MUST NOT carry a fragment (RFC 9728
            §3 canonical-URI rules — callers should validate upstream).
        authorization_server_issuer_url: The AS's issuer URL, placed in
            ``authorization_servers[0]``. Multiple ASes can be listed by
            the caller populating the list form directly on the returned
            dict.
        scopes_supported: Optional list advertised in ``scopes_supported``
            per RFC 9728 §2. Omitted from the JSON when ``None`` or empty.
        advertise_asymmetric_algs: When true (default), the document
            advertises ``resource_signing_alg_values_supported:
            ["RS256","ES256"]`` — matches the enforcement policy in
            :class:`~looker_mcp_server.oidc.resource_server.OAuth21ResourceServer`.
        resource_name: Optional human-readable label.
        resource_documentation: Optional URL pointing at human docs.

    Returns:
        A dict ready for ``json.dumps`` / FastAPI/Starlette JSON response.
    """
    if not resource_uri:
        raise ValueError("resource_uri must not be empty")
    if not authorization_server_issuer_url:
        raise ValueError("authorization_server_issuer_url must not be empty")

    model = ProtectedResourceMetadata(
        resource=resource_uri,
        authorization_servers=[authorization_server_issuer_url],
        scopes_supported=scopes_supported or None,
        resource_signing_alg_values_supported=(
            ["RS256", "ES256"] if advertise_asymmetric_algs else None
        ),
        resource_name=resource_name,
        resource_documentation=resource_documentation,
    )
    # exclude_none: don't emit `null` fields per RFC 9728 idiomatic shape.
    return model.model_dump(exclude_none=True)
