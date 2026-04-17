"""OAuth 2.1 access-token validator — the resource-server side of OIDC mode.

Accepts a Bearer JWT, resolves its signing key via a :class:`JWKSCache`,
and validates the signature, expiry, issuer, and audience. RS256/ES256
only (RFC 9068 §2.1); HS256 is rejected by the allowlist even if the
attacker manages to mint an HS-signed token, closing the classic
algorithm-confusion attack surface.

The validator is deliberately split from the HTTP layer so it can be
reused by middleware, unit tests, and out-of-band tooling (e.g. an
auditor confirming a minted token verifies against the published JWKS).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

import jwt as pyjwt

from .jwks import ALLOWED_SIGNING_ALGORITHMS, JWKSCache, JWKSError

logger = logging.getLogger(__name__)


class TokenVerificationError(Exception):
    """Raised when an access token fails verification.

    Deliberately does NOT subclass :class:`jwt.InvalidTokenError` — the
    error surface here is narrower (no PyJWT subtype leak into callers)
    and the category (``invalid_token`` per RFC 6750 §3.1) is
    orthogonal to whatever specific JWT check failed.
    """


@dataclass(frozen=True)
class VerifiedClaims:
    """The outcome of a successful token verification.

    Carries the JOSE header fields the caller might need (``kid``, ``alg``)
    alongside the decoded claim set. The raw JWT string is intentionally
    not stored — callers that need it already have it in the request.
    """

    kid: str
    alg: str
    claims: dict[str, Any] = field(default_factory=dict)

    @property
    def sub(self) -> str | None:
        return self.claims.get("sub")

    @property
    def scopes(self) -> list[str]:
        raw = self.claims.get("scope")
        if isinstance(raw, str):
            return raw.split()
        if isinstance(raw, list):
            return [s for s in raw if isinstance(s, str)]
        return []


class OAuth21ResourceServer:
    """OAuth 2.1 resource-server token validator.

    Args:
        jwks: A :class:`JWKSCache` keyed on the authorization server's
            ``jwks_uri``.
        issuer: Expected ``iss`` claim (RFC 8414).
        audience: Expected ``aud`` claim — the resource server's canonical
            URI (RFC 8707 §2). Audience binding prevents a token minted
            for one resource from being replayed against another.
        leeway_seconds: Clock-skew tolerance passed to PyJWT's exp/iat
            checks. Default 30 seconds — mirrors common RFC-9068 guidance.
    """

    def __init__(
        self,
        jwks: JWKSCache,
        *,
        issuer: str,
        audience: str,
        leeway_seconds: float = 30.0,
    ) -> None:
        if not issuer:
            raise ValueError("issuer must not be empty")
        if not audience:
            raise ValueError("audience must not be empty")
        self._jwks = jwks
        self._issuer = issuer
        self._audience = audience
        self._leeway = leeway_seconds

    async def verify(self, token: str) -> VerifiedClaims:
        """Validate a Bearer ``token``. Returns :class:`VerifiedClaims`.

        Raises:
            TokenVerificationError: On any verification failure — missing
                kid, unsupported alg, unknown kid, bad signature, expired,
                wrong issuer, wrong audience, etc. The error message is
                intentionally coarse so audit logs don't leak structure
                about which specific check failed to an unauthenticated
                caller.
        """
        if not token:
            raise TokenVerificationError("empty token")

        try:
            header = pyjwt.get_unverified_header(token)
        except pyjwt.InvalidTokenError as exc:
            raise TokenVerificationError("malformed token") from exc

        alg = header.get("alg")
        if not isinstance(alg, str) or alg not in ALLOWED_SIGNING_ALGORITHMS:
            # RFC 9068 §2.1: symmetric algorithms are forbidden for access
            # tokens. Rejecting here closes the algorithm-confusion attack
            # surface where an attacker HMAC-signs a token with the
            # published public key as the secret.
            raise TokenVerificationError(
                f"unsupported or missing alg (allowed: {sorted(ALLOWED_SIGNING_ALGORITHMS)})"
            )

        kid = header.get("kid")
        if not isinstance(kid, str) or not kid:
            raise TokenVerificationError("token missing kid header")

        try:
            jwk = await self._jwks.get_key(kid)
        except JWKSError as exc:
            raise TokenVerificationError(f"key lookup failed: {exc}") from exc

        try:
            claims = pyjwt.decode(
                token,
                key=jwk.key,  # type: ignore[arg-type]
                algorithms=[alg],
                audience=self._audience,
                issuer=self._issuer,
                leeway=self._leeway,
                options={
                    "require": ["exp", "iat", "iss", "aud", "sub"],
                    "verify_aud": True,
                    "verify_iss": True,
                    "verify_exp": True,
                    "verify_iat": True,
                    "verify_signature": True,
                },
            )
        except pyjwt.InvalidTokenError as exc:
            # Narrow-message: "invalid token" is the only thing the 401
            # audit record should surface. The specific reason lives in
            # the internal log.
            logger.info(
                "oidc.token.rejected",
                extra={"reason": type(exc).__name__, "detail": str(exc)},
            )
            raise TokenVerificationError("invalid token") from exc

        if not isinstance(claims, dict):  # pragma: no cover — PyJWT returns dict
            raise TokenVerificationError("invalid token payload")

        return VerifiedClaims(kid=kid, alg=alg, claims=claims)
