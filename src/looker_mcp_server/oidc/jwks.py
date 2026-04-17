"""JWKS (RFC 7517) key cache with TTL-based refresh + throttled kid-miss.

Resource servers validating OAuth 2.1 access tokens need the authorization
server's public keys.  Fetching on every request is wasteful; fetching
without bounds on repeated ``kid`` misses is a DoS vector against the AS.

:class:`JWKSCache` balances both:

- First resolution and every TTL expiry refresh the cache once, guarded by
  an async lock so concurrent requests don't thundering-herd.
- A token arriving with a ``kid`` the cache doesn't know triggers at most
  one forced refresh per cooldown period (default: 5 minutes).  This
  accommodates key rotations without flooding the AS on brute-force
  attempts with random ``kid`` values.
- Cold-start fetch failures are re-raised — the resource server must fail
  closed rather than admit unverified tokens.

Only asymmetric signing algorithms are accepted (RFC 9068 §2.1).  Symmetric
algorithms expose the classic algorithm-confusion attack where an
attacker re-signs a token with the fetched public key as an HMAC secret.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

import httpx
from jwt import PyJWK

logger = logging.getLogger(__name__)

#: Asymmetric signing algorithms this cache will accept from JWKS entries.
#: HS* is intentionally excluded — RFC 9068 §2.1 forbids symmetric signing
#: for OAuth 2.1 access tokens, and accepting public keys here under an
#: HS alg opens the algorithm-confusion attack surface.
ALLOWED_SIGNING_ALGORITHMS: frozenset[str] = frozenset(
    {"RS256", "RS384", "RS512", "ES256", "ES384", "ES512"}
)


class JWKSError(Exception):
    """Raised when a JWKS operation fails (fetch error, invalid payload,
    or requested ``kid`` absent even after a forced refresh)."""


class JWKSCache:
    """Async JWKS key cache.

    Instance-based (not module-global) so a process hosting multiple
    resource servers against different authorization servers gets one
    cache per AS with independent TTLs and locks.

    Args:
        jwks_uri: URL of the JWK Set document.
        ttl_seconds: Lifetime of a cache entry. Default 1 hour.
        kid_miss_cooldown_seconds: Minimum spacing between forced refreshes
            triggered by unknown ``kid`` values. Default 5 minutes.
        http_timeout_seconds: Per-request HTTP timeout when fetching the
            JWKS. Default 10 seconds.
    """

    def __init__(
        self,
        jwks_uri: str,
        *,
        ttl_seconds: float = 3600.0,
        kid_miss_cooldown_seconds: float = 300.0,
        http_timeout_seconds: float = 10.0,
    ) -> None:
        if not jwks_uri:
            raise ValueError("jwks_uri must not be empty")
        self._jwks_uri = jwks_uri
        self._ttl = ttl_seconds
        self._cooldown = kid_miss_cooldown_seconds
        self._http_timeout = http_timeout_seconds

        self._lock = asyncio.Lock()
        self._keys: dict[str, PyJWK] = {}
        self._fetched_at: float = 0.0
        self._last_forced_refresh: float = 0.0

    @property
    def jwks_uri(self) -> str:
        return self._jwks_uri

    async def get_key(self, kid: str) -> PyJWK:
        """Return the :class:`PyJWK` matching ``kid``, refreshing if needed.

        Raises:
            JWKSError: If the key cannot be located even after a forced
                refresh, or the underlying HTTP fetch fails.
        """
        if not kid:
            raise JWKSError("kid must not be empty")

        # Fast path: cache warm and kid known.
        if self._is_fresh() and kid in self._keys:
            return self._keys[kid]

        async with self._lock:
            # Re-check under lock — another coroutine may have refreshed
            # while we were waiting.
            if self._is_fresh() and kid in self._keys:
                return self._keys[kid]

            # Cold or stale cache: refresh if the TTL has elapsed.
            if not self._is_fresh():
                await self._refresh()
                if kid in self._keys:
                    return self._keys[kid]

            # Cache is fresh but the kid isn't known — rotation case.
            # Force a refresh if we haven't done one recently.
            now = time.monotonic()
            if now - self._last_forced_refresh >= self._cooldown:
                await self._refresh()
                self._last_forced_refresh = now
                if kid in self._keys:
                    return self._keys[kid]

            known = sorted(self._keys.keys())
            raise JWKSError(
                f"no key with kid={kid!r} in JWKS (known kids: {known}); "
                f"forced-refresh throttled to once per {self._cooldown:g}s"
            )

    def _is_fresh(self) -> bool:
        return (time.monotonic() - self._fetched_at) < self._ttl

    async def _refresh(self) -> None:
        logger.debug("jwks.refresh.start", extra={"jwks_uri": self._jwks_uri})
        try:
            async with httpx.AsyncClient(timeout=self._http_timeout) as client:
                resp = await client.get(self._jwks_uri)
                resp.raise_for_status()
                payload: Any = resp.json()
        except httpx.HTTPError as exc:
            logger.warning(
                "jwks.refresh.failed",
                extra={"jwks_uri": self._jwks_uri, "error": str(exc)},
            )
            # If we have ANY cached keys, preserve them — the caller can
            # still validate tokens whose kid is already known. If we
            # don't, re-raise so the caller can fail closed at cold start.
            if not self._keys:
                raise JWKSError(f"failed to fetch JWKS: {exc}") from exc
            return

        if not isinstance(payload, dict) or not isinstance(payload.get("keys"), list):
            raise JWKSError(
                f"JWKS response at {self._jwks_uri} is not a valid RFC 7517 "
                f"document (missing 'keys' array)"
            )

        new_keys: dict[str, PyJWK] = {}
        for raw in payload["keys"]:
            if not isinstance(raw, dict):
                continue
            alg = raw.get("alg")
            if alg and alg not in ALLOWED_SIGNING_ALGORITHMS:
                # Silently skip entries the caller couldn't safely verify
                # against anyway — a log at debug level so operators can
                # spot AS configs that advertise mixed algorithms.
                logger.debug(
                    "jwks.refresh.skip_unsupported_alg",
                    extra={"alg": alg, "kid": raw.get("kid")},
                )
                continue
            kid = raw.get("kid")
            if not kid:
                continue
            try:
                new_keys[kid] = PyJWK.from_dict(raw)
            except Exception as exc:  # pragma: no cover — PyJWT raises various subtypes
                logger.warning(
                    "jwks.refresh.skip_unparseable",
                    extra={"kid": kid, "error": str(exc)},
                )

        if not new_keys:
            raise JWKSError(
                f"JWKS at {self._jwks_uri} contains no usable "
                f"asymmetric keys (allowed: {sorted(ALLOWED_SIGNING_ALGORITHMS)})"
            )

        self._keys = new_keys
        self._fetched_at = time.monotonic()
        logger.debug(
            "jwks.refresh.success",
            extra={"jwks_uri": self._jwks_uri, "key_count": len(new_keys)},
        )
