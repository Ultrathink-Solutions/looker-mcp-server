"""OIDC / OAuth 2.1 resource-server primitives for ``LOOKER_MCP_MODE=public``.

Modules:

- :mod:`looker_mcp_server.oidc.jwks` — async JWKS (RFC 7517) key cache with
  TTL, async-lock-guarded refresh, and throttled kid-miss forced-refresh.
- :mod:`looker_mcp_server.oidc.resource_server` — OAuth 2.1 access-token
  validator. ``RS256``/``ES256`` only (RFC 9068 §2.1). Audience binding
  per RFC 8707 §2. Issuer binding per RFC 8414.
- :mod:`looker_mcp_server.oidc.prm` — Protected Resource Metadata document
  (RFC 9728 §2) + route factory.
- :mod:`looker_mcp_server.oidc.www_authenticate` — 401/403 challenge
  builders. ``realm=`` is mandatory per RFC 7235 §4.1; ``quoted-string``
  escaping follows RFC 7230 §3.2.6.

Nothing in this package imports Looker-specific types — the primitives
are vendor-neutral and suitable for reuse by adopters building their own
resource servers against the same spec family.
"""

from __future__ import annotations

from .jwks import ALLOWED_SIGNING_ALGORITHMS, JWKSCache, JWKSError
from .middleware import PublicModeAuthMiddleware
from .prm import ProtectedResourceMetadata, build_prm_document
from .resource_server import (
    OAuth21ResourceServer,
    TokenVerificationError,
    VerifiedClaims,
)
from .www_authenticate import (
    escape_quoted_string,
    insufficient_scope_challenge,
    invalid_token_challenge,
)

__all__ = [
    "ALLOWED_SIGNING_ALGORITHMS",
    "JWKSCache",
    "JWKSError",
    "OAuth21ResourceServer",
    "ProtectedResourceMetadata",
    "PublicModeAuthMiddleware",
    "TokenVerificationError",
    "VerifiedClaims",
    "build_prm_document",
    "escape_quoted_string",
    "insufficient_scope_challenge",
    "invalid_token_challenge",
]
