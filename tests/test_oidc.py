"""Tests for the OIDC resource-server primitives.

Covers:
- :mod:`looker_mcp_server.oidc.www_authenticate` — challenge strings +
  RFC 7230 quoted-string escaping
- :mod:`looker_mcp_server.oidc.prm` — PRM document shape (RFC 9728 §2)
- :mod:`looker_mcp_server.oidc.jwks` — TTL, kid-miss throttle, fail-closed
  cold start
- :mod:`looker_mcp_server.oidc.resource_server` — algorithm allowlist,
  issuer + audience binding, kid routing, round-trip happy path
"""

from __future__ import annotations

import time

import httpx
import jwt as pyjwt
import pytest
import respx
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec, rsa

from looker_mcp_server.oidc import (
    JWKSCache,
    JWKSError,
    OAuth21ResourceServer,
    TokenVerificationError,
    build_prm_document,
    escape_quoted_string,
    insufficient_scope_challenge,
    invalid_token_challenge,
)

# ---------------------------------------------------------------------------
# Keypair + JWKS fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def rsa_keypair():
    private = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    private_pem = private.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode("ascii")
    public_pem = (
        private.public_key()
        .public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        .decode("ascii")
    )
    return {
        "private_pem": private_pem,
        "public_pem": public_pem,
        "public_key": private.public_key(),
    }


@pytest.fixture(scope="module")
def ec_keypair():
    """P-256 EC keypair for ES256 signing — parity with ``rsa_keypair``."""
    private = ec.generate_private_key(ec.SECP256R1())
    private_pem = private.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode("ascii")
    return {
        "private_pem": private_pem,
        "public_key": private.public_key(),
    }


def _make_jwks(public_key, kid: str = "test-kid-1") -> dict:
    from jwt.algorithms import RSAAlgorithm

    jwk = RSAAlgorithm.to_jwk(public_key, as_dict=True)
    jwk.update({"kid": kid, "use": "sig", "alg": "RS256"})
    return {"keys": [jwk]}


def _make_ec_jwks(public_key, kid: str = "test-kid-es256") -> dict:
    from jwt.algorithms import ECAlgorithm

    jwk = ECAlgorithm.to_jwk(public_key, as_dict=True)
    jwk.update({"kid": kid, "use": "sig", "alg": "ES256"})
    return {"keys": [jwk]}


def _mint(
    *,
    private_pem: str,
    kid: str = "test-kid-1",
    iss: str = "https://as.example.com",
    aud: str = "https://looker.example.com/mcp",
    sub: str = "user-1",
    ttl: int = 3600,
    alg: str = "RS256",
) -> str:
    now = int(time.time())
    return pyjwt.encode(
        {
            "iss": iss,
            "aud": aud,
            "sub": sub,
            "iat": now,
            "exp": now + ttl,
            "scope": "looker:read",
        },
        private_pem,
        algorithm=alg,
        headers={"kid": kid},
    )


# ---------------------------------------------------------------------------
# www_authenticate
# ---------------------------------------------------------------------------


class TestEscapeQuotedString:
    def test_plain_unchanged(self):
        assert escape_quoted_string("plain") == "plain"

    def test_quote_escaped(self):
        assert escape_quoted_string('he said "hi"') == 'he said \\"hi\\"'

    def test_backslash_escaped(self):
        assert escape_quoted_string("path\\to") == "path\\\\to"

    def test_both_escaped(self):
        assert escape_quoted_string('a"b\\c') == 'a\\"b\\\\c'

    def test_empty(self):
        assert escape_quoted_string("") == ""


class TestInvalidTokenChallenge:
    def test_shape(self):
        value = invalid_token_challenge(
            realm="https://looker.example.com",
            resource_metadata_url="https://looker.example.com/.well-known/oauth-protected-resource",
        )
        assert value.startswith("Bearer ")
        assert 'realm="https://looker.example.com"' in value
        assert (
            'resource_metadata="https://looker.example.com/.well-known/oauth-protected-resource"'
            in value
        )

    def test_empty_realm_rejected(self):
        with pytest.raises(ValueError, match="realm"):
            invalid_token_challenge(
                realm="",
                resource_metadata_url="https://example.com/md",
            )


class TestInsufficientScopeChallenge:
    def test_shape_with_scopes(self):
        value = insufficient_scope_challenge(
            realm="https://looker.example.com",
            required_scopes=["looker:read", "looker:write"],
        )
        assert 'error="insufficient_scope"' in value
        assert 'scope="looker:read looker:write"' in value

    def test_scope_omitted_when_empty(self):
        value = insufficient_scope_challenge(realm="https://x.example", required_scopes=None)
        assert "scope=" not in value
        assert 'error="insufficient_scope"' in value


# ---------------------------------------------------------------------------
# PRM
# ---------------------------------------------------------------------------


class TestBuildPrmDocument:
    def test_minimal_shape(self):
        doc = build_prm_document(
            resource_uri="https://looker.example.com/mcp",
            authorization_server_issuer_url="https://as.example.com",
        )
        assert doc["resource"] == "https://looker.example.com/mcp"
        assert doc["authorization_servers"] == ["https://as.example.com"]
        assert doc["bearer_methods_supported"] == ["header"]
        # Defaults: asymmetric advertising ON, scopes unset, name/docs unset.
        assert doc["resource_signing_alg_values_supported"] == ["RS256", "ES256"]
        assert "scopes_supported" not in doc
        assert "resource_name" not in doc

    def test_full_shape(self):
        doc = build_prm_document(
            resource_uri="https://looker.example.com/mcp",
            authorization_server_issuer_url="https://as.example.com",
            scopes_supported=["looker:read", "looker:write"],
            resource_name="Looker MCP",
            resource_documentation="https://docs.example.com/looker-mcp",
        )
        assert doc["scopes_supported"] == ["looker:read", "looker:write"]
        assert doc["resource_name"] == "Looker MCP"
        assert doc["resource_documentation"] == "https://docs.example.com/looker-mcp"

    def test_opt_out_of_alg_advertising(self):
        """If the AS is still mid-migration to asymmetric signing, the caller
        can refuse to advertise RS256/ES256 support yet — truth in advertising."""
        doc = build_prm_document(
            resource_uri="https://x.example/mcp",
            authorization_server_issuer_url="https://as.example",
            advertise_asymmetric_algs=False,
        )
        assert "resource_signing_alg_values_supported" not in doc

    def test_empty_resource_uri_rejected(self):
        with pytest.raises(ValueError, match="resource_uri"):
            build_prm_document(
                resource_uri="",
                authorization_server_issuer_url="https://as.example",
            )


# ---------------------------------------------------------------------------
# JWKS cache
# ---------------------------------------------------------------------------


class TestJWKSCache:
    def test_empty_jwks_uri_rejected(self):
        with pytest.raises(ValueError, match="jwks_uri"):
            JWKSCache("")

    @pytest.mark.asyncio
    @respx.mock
    async def test_happy_path_caches_and_returns_key(self, rsa_keypair):
        cache = JWKSCache("https://as.example.com/.well-known/jwks.json")
        route = respx.get(cache.jwks_uri).mock(
            return_value=httpx.Response(200, json=_make_jwks(rsa_keypair["public_key"]))
        )

        jwk = await cache.get_key("test-kid-1")
        assert jwk.key_id == "test-kid-1"

        # Second call should hit the cache — route receives only one call.
        await cache.get_key("test-kid-1")
        assert route.call_count == 1

    @pytest.mark.asyncio
    @respx.mock
    async def test_unknown_kid_forces_one_refresh_then_raises(self, rsa_keypair):
        cache = JWKSCache(
            "https://as.example.com/.well-known/jwks.json",
            kid_miss_cooldown_seconds=60,
        )
        route = respx.get(cache.jwks_uri).mock(
            return_value=httpx.Response(200, json=_make_jwks(rsa_keypair["public_key"]))
        )

        with pytest.raises(JWKSError, match="no key with kid"):
            await cache.get_key("unknown-kid")

        # First call: cold load (fetches once). kid miss then triggers a
        # forced refresh (second fetch). Both fail to find the kid.
        assert route.call_count == 2

    @pytest.mark.asyncio
    @respx.mock
    async def test_cold_start_failure_raises(self):
        cache = JWKSCache("https://as.example.com/.well-known/jwks.json")
        respx.get(cache.jwks_uri).mock(return_value=httpx.Response(503))

        with pytest.raises(JWKSError, match="failed to fetch"):
            await cache.get_key("any-kid")

    @pytest.mark.asyncio
    @respx.mock
    async def test_symmetric_keys_rejected_from_jwks(self):
        """A JWKS entry advertising HS256 is silently dropped (not used for
        verification) — defence in depth against an AS that serves mixed algs."""
        cache = JWKSCache("https://as.example.com/.well-known/jwks.json")
        respx.get(cache.jwks_uri).mock(
            return_value=httpx.Response(
                200,
                json={"keys": [{"kty": "oct", "alg": "HS256", "kid": "hs-1", "k": "c2VjcmV0"}]},
            )
        )
        with pytest.raises(JWKSError, match="no usable asymmetric keys"):
            await cache.get_key("hs-1")

    @pytest.mark.asyncio
    @respx.mock
    async def test_narrow_alg_allowlist_rejects_rs384(self, rsa_keypair):
        """ALLOWED_SIGNING_ALGORITHMS is narrow (RS256/ES256 only). An IdP
        serving an RS384 key is not silently accepted — the operator must
        opt into widening the allowlist explicitly."""
        from jwt.algorithms import RSAAlgorithm

        jwk = RSAAlgorithm.to_jwk(rsa_keypair["public_key"], as_dict=True)
        jwk.update({"kid": "rs384-key", "use": "sig", "alg": "RS384"})
        cache = JWKSCache("https://as.example.com/.well-known/jwks.json")
        respx.get(cache.jwks_uri).mock(return_value=httpx.Response(200, json={"keys": [jwk]}))
        with pytest.raises(JWKSError, match="no usable asymmetric keys"):
            await cache.get_key("rs384-key")

    @pytest.mark.asyncio
    @respx.mock
    async def test_kty_mismatch_dropped(self, rsa_keypair):
        """An RS256-advertised key with kty=EC is a misconfiguration or an
        algorithm-confusion probe — skip rather than import."""
        from jwt.algorithms import RSAAlgorithm

        rsa_jwk = RSAAlgorithm.to_jwk(rsa_keypair["public_key"], as_dict=True)
        # Fake an RS256-labelled entry but lie about the kty.
        bad_jwk = dict(rsa_jwk)
        bad_jwk.update({"kid": "mismatch-1", "use": "sig", "alg": "RS256", "kty": "EC"})
        cache = JWKSCache("https://as.example.com/.well-known/jwks.json")
        respx.get(cache.jwks_uri).mock(return_value=httpx.Response(200, json={"keys": [bad_jwk]}))
        with pytest.raises(JWKSError, match="no usable asymmetric keys"):
            await cache.get_key("mismatch-1")

    @pytest.mark.asyncio
    @respx.mock
    async def test_invalid_json_after_success_preserves_cache(self, rsa_keypair):
        """Post-success transient AS failures (malformed JSON body) must
        not wipe the existing cache — in-flight tokens keep verifying."""
        cache = JWKSCache(
            "https://as.example.com/.well-known/jwks.json",
            ttl_seconds=0.0,  # immediately stale so the next call forces _refresh
        )
        route = respx.get(cache.jwks_uri).mock(
            side_effect=[
                httpx.Response(200, json=_make_jwks(rsa_keypair["public_key"])),
                httpx.Response(200, content=b"not-json"),
            ]
        )

        jwk = await cache.get_key("test-kid-1")
        assert jwk.key_id == "test-kid-1"

        # Second call triggers a refresh (TTL=0) against a broken JSON body.
        # The existing cache entry must still resolve.
        jwk2 = await cache.get_key("test-kid-1")
        assert jwk2.key_id == "test-kid-1"
        assert route.call_count == 2

    @pytest.mark.asyncio
    @respx.mock
    async def test_zero_usable_keys_after_success_preserves_cache(self, rsa_keypair):
        """Same contract as the JSON-parse case: an AS that goes through a
        bad-config window and returns `{"keys": []}` must not invalidate
        keys the cache has already admitted."""
        cache = JWKSCache(
            "https://as.example.com/.well-known/jwks.json",
            ttl_seconds=0.0,
        )
        route = respx.get(cache.jwks_uri).mock(
            side_effect=[
                httpx.Response(200, json=_make_jwks(rsa_keypair["public_key"])),
                httpx.Response(200, json={"keys": []}),
            ]
        )

        jwk = await cache.get_key("test-kid-1")
        assert jwk.key_id == "test-kid-1"

        jwk2 = await cache.get_key("test-kid-1")
        assert jwk2.key_id == "test-kid-1"
        assert route.call_count == 2

    @pytest.mark.asyncio
    @respx.mock
    async def test_invalid_payload_shape_cold_start_fails_closed(self):
        """Contrast to the post-success tests above: if the AS is broken
        on the FIRST fetch (cold start), raise so the caller fails closed
        rather than silently accept an empty cache."""
        cache = JWKSCache("https://as.example.com/.well-known/jwks.json")
        respx.get(cache.jwks_uri).mock(return_value=httpx.Response(200, json={"not_keys": 123}))
        with pytest.raises(JWKSError, match="not a valid RFC 7517"):
            await cache.get_key("any-kid")


# ---------------------------------------------------------------------------
# Resource server validator
# ---------------------------------------------------------------------------


class TestOAuth21ResourceServer:
    def _make_validator(self, rsa_keypair):
        """Configure a JWKS mock (caller must be inside @respx.mock scope)
        and return a validator ready to exercise."""
        cache = JWKSCache("https://as.example.com/.well-known/jwks.json")
        respx.get(cache.jwks_uri).mock(
            return_value=httpx.Response(200, json=_make_jwks(rsa_keypair["public_key"]))
        )
        return OAuth21ResourceServer(
            cache,
            issuer="https://as.example.com",
            audience="https://looker.example.com/mcp",
        )

    @pytest.mark.asyncio
    @respx.mock
    async def test_happy_path_rs256(self, rsa_keypair):
        validator = self._make_validator(rsa_keypair)
        token = _mint(private_pem=rsa_keypair["private_pem"])

        verified = await validator.verify(token)
        assert verified.sub == "user-1"
        assert verified.kid == "test-kid-1"
        assert verified.alg == "RS256"
        assert verified.scopes == ["looker:read"]

    @pytest.mark.asyncio
    @respx.mock
    async def test_happy_path_es256(self, ec_keypair):
        """ES256 is the second algorithm advertised in the PRM and in
        ``ALLOWED_SIGNING_ALGORITHMS``; this test pins the happy-path
        verify so a regression in EC key handling (JWK import or jose
        selection) gets caught alongside the RS256 path."""
        cache = JWKSCache("https://as.example.com/.well-known/jwks.json")
        respx.get(cache.jwks_uri).mock(
            return_value=httpx.Response(200, json=_make_ec_jwks(ec_keypair["public_key"]))
        )
        validator = OAuth21ResourceServer(
            cache,
            issuer="https://as.example.com",
            audience="https://looker.example.com/mcp",
        )
        token = _mint(
            private_pem=ec_keypair["private_pem"],
            kid="test-kid-es256",
            alg="ES256",
        )

        verified = await validator.verify(token)
        assert verified.sub == "user-1"
        assert verified.kid == "test-kid-es256"
        assert verified.alg == "ES256"
        assert verified.scopes == ["looker:read"]

    @pytest.mark.asyncio
    @respx.mock
    async def test_hs256_token_rejected(self, rsa_keypair):
        """Even if an attacker mints an HS256 token with any secret, the
        algorithm allowlist rejects it at the header-inspection stage —
        classic algorithm-confusion defense (RFC 9068 §2.1)."""
        validator = self._make_validator(rsa_keypair)
        attacker_token = pyjwt.encode(
            {"iss": "https://as.example.com", "aud": "https://looker.example.com/mcp", "sub": "x"},
            "attacker-secret",
            algorithm="HS256",
        )
        with pytest.raises(TokenVerificationError, match="unsupported or missing alg"):
            await validator.verify(attacker_token)

    @pytest.mark.asyncio
    @respx.mock
    async def test_wrong_audience_rejected(self, rsa_keypair):
        validator = self._make_validator(rsa_keypair)
        token = _mint(
            private_pem=rsa_keypair["private_pem"],
            aud="https://someone-else.example/mcp",
        )
        with pytest.raises(TokenVerificationError, match="invalid token"):
            await validator.verify(token)

    @pytest.mark.asyncio
    @respx.mock
    async def test_wrong_issuer_rejected(self, rsa_keypair):
        validator = self._make_validator(rsa_keypair)
        token = _mint(
            private_pem=rsa_keypair["private_pem"],
            iss="https://other-as.example.com",
        )
        with pytest.raises(TokenVerificationError, match="invalid token"):
            await validator.verify(token)

    @pytest.mark.asyncio
    @respx.mock
    async def test_expired_rejected(self, rsa_keypair):
        validator = self._make_validator(rsa_keypair)
        token = _mint(private_pem=rsa_keypair["private_pem"], ttl=-60)
        with pytest.raises(TokenVerificationError, match="invalid token"):
            await validator.verify(token)

    @pytest.mark.asyncio
    @respx.mock
    async def test_forged_with_different_key_rejected(self, rsa_keypair):
        """A token signed with an UNRELATED RSA key fails signature check
        even if its kid matches the one published."""
        validator = self._make_validator(rsa_keypair)
        other = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        other_pem = other.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        ).decode("ascii")
        token = _mint(private_pem=other_pem)
        with pytest.raises(TokenVerificationError, match="invalid token"):
            await validator.verify(token)

    @pytest.mark.asyncio
    @respx.mock
    async def test_missing_kid_header_rejected(self, rsa_keypair):
        validator = self._make_validator(rsa_keypair)
        token = pyjwt.encode(
            {"iss": "https://as.example.com", "aud": "https://looker.example.com/mcp", "sub": "x"},
            rsa_keypair["private_pem"],
            algorithm="RS256",
            # No headers={"kid": ...} — deliberate omission.
        )
        with pytest.raises(TokenVerificationError, match="missing kid"):
            await validator.verify(token)

    @pytest.mark.asyncio
    @respx.mock
    async def test_empty_token_rejected(self, rsa_keypair):
        validator = self._make_validator(rsa_keypair)
        with pytest.raises(TokenVerificationError, match="empty token"):
            await validator.verify("")

    def test_construct_empty_issuer_rejected(self):
        """No respx mock needed — construction fails before any HTTP call."""
        cache = JWKSCache("https://as.example.com/.well-known/jwks.json")
        with pytest.raises(ValueError, match="issuer"):
            OAuth21ResourceServer(cache, issuer="", audience="https://x/")
