"""Tests for :class:`PublicModeAuthMiddleware` — the ASGI auth gate."""

from __future__ import annotations

import json
import time

import jwt as pyjwt
import pytest
import respx
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from looker_mcp_server.oidc import JWKSCache, OAuth21ResourceServer
from looker_mcp_server.oidc.middleware import PublicModeAuthMiddleware


@pytest.fixture(scope="module")
def rsa_keypair():
    pk = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = pk.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode("ascii")
    return {"private_pem": pem, "public_key": pk.public_key()}


def _jwks_body(public_key, kid: str = "k1") -> dict:
    from jwt.algorithms import RSAAlgorithm

    jwk = RSAAlgorithm.to_jwk(public_key, as_dict=True)
    jwk.update({"kid": kid, "use": "sig", "alg": "RS256"})
    return {"keys": [jwk]}


def _mint(private_pem: str, *, aud: str, iss: str, kid: str = "k1", ttl: int = 3600) -> str:
    now = int(time.time())
    return pyjwt.encode(
        {"iss": iss, "aud": aud, "sub": "user-1", "iat": now, "exp": now + ttl},
        private_pem,
        algorithm="RS256",
        headers={"kid": kid},
    )


def _build_resource_server(rsa_keypair) -> OAuth21ResourceServer:
    """Set up the JWKS mock + return a validator.

    The caller MUST be inside ``@respx.mock`` scope (the repo convention;
    the alternative ``respx_mock`` fixture isn't reliably registered
    across CI Python versions).  Uses ``.respond(...)`` rather than
    ``.mock(return_value=Response(...))`` per pytest-respx idiomatic style.
    """
    cache = JWKSCache("https://as.example.com/.well-known/jwks.json")
    respx.get(cache.jwks_uri).respond(status_code=200, json=_jwks_body(rsa_keypair["public_key"]))
    return OAuth21ResourceServer(
        cache,
        issuer="https://as.example.com",
        audience="https://looker.example.com/mcp",
    )


async def _echo_app(scope, receive, send):
    """Tiny ASGI app: emits 200 with the verified sub claim (if present)."""
    verified = (scope.get("state") or {}).get("verified_claims")
    body = json.dumps(
        {"authenticated_sub": verified.sub if verified is not None else None}
    ).encode()
    await send(
        {
            "type": "http.response.start",
            "status": 200,
            "headers": [(b"content-type", b"application/json")],
        }
    )
    await send({"type": "http.response.body", "body": body})


async def _drive(
    mw, *, method: str = "GET", path: str = "/mcp", headers=None, query_string: bytes = b""
):
    """Minimal ASGI driver — return (status, response_headers_dict, body_bytes)."""
    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "method": method,
        "path": path,
        "raw_path": path.encode(),
        "query_string": query_string,
        "headers": [(k.encode().lower(), v.encode()) for k, v in (headers or {}).items()],
    }
    received = {"status": None, "headers": {}, "body": b""}

    async def _recv():
        return {"type": "http.request", "body": b"", "more_body": False}

    async def _send(message):
        if message["type"] == "http.response.start":
            received["status"] = message["status"]
            received["headers"] = {k.decode(): v.decode() for k, v in message["headers"]}
        elif message["type"] == "http.response.body":
            received["body"] += message.get("body", b"")

    await mw(scope, _recv, _send)
    return received["status"], received["headers"], received["body"]


class TestPublicModeAuthMiddleware:
    def _mw(self, resource_server) -> PublicModeAuthMiddleware:
        return PublicModeAuthMiddleware(
            _echo_app,
            resource_server=resource_server,
            realm="https://looker.example.com",
            prm_url="https://looker.example.com/.well-known/oauth-protected-resource",
        )

    @respx.mock
    async def test_valid_token_passes_through(self, rsa_keypair):
        mw = self._mw(_build_resource_server(rsa_keypair))
        token = _mint(
            rsa_keypair["private_pem"],
            aud="https://looker.example.com/mcp",
            iss="https://as.example.com",
        )
        status, _, body = await _drive(mw, headers={"Authorization": f"Bearer {token}"})
        assert status == 200
        assert json.loads(body) == {"authenticated_sub": "user-1"}

    @respx.mock
    async def test_missing_authorization_401_with_challenge(self, rsa_keypair):
        mw = self._mw(_build_resource_server(rsa_keypair))
        status, headers, body = await _drive(mw)
        assert status == 401
        www = headers.get("www-authenticate") or ""
        assert www.startswith("Bearer ")
        assert 'realm="https://looker.example.com"' in www
        assert (
            'resource_metadata="https://looker.example.com/.well-known/oauth-protected-resource"'
            in www
        )
        assert json.loads(body)["error"] == "invalid_token"

    @respx.mock
    async def test_malformed_bearer_prefix_401(self, rsa_keypair):
        mw = self._mw(_build_resource_server(rsa_keypair))
        status, _, _ = await _drive(mw, headers={"Authorization": "Basic abcd"})
        assert status == 401

    @respx.mock
    async def test_invalid_token_401(self, rsa_keypair):
        """Token signed with a foreign key fails signature verification."""
        mw = self._mw(_build_resource_server(rsa_keypair))
        other = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        other_pem = other.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        ).decode("ascii")
        forged = _mint(
            other_pem,
            aud="https://looker.example.com/mcp",
            iss="https://as.example.com",
        )
        status, _, body = await _drive(mw, headers={"Authorization": f"Bearer {forged}"})
        assert status == 401
        assert json.loads(body)["error"] == "invalid_token"

    @respx.mock
    async def test_bearer_in_query_rejected_with_400(self, rsa_keypair):
        mw = self._mw(_build_resource_server(rsa_keypair))
        status, headers, body = await _drive(mw, query_string=b"access_token=attacker")
        assert status == 400
        assert headers.get("content-type") == "application/json"
        parsed = json.loads(body)
        assert parsed["error"] == "invalid_request"
        assert "OAuth 2.1" in parsed["error_description"]

    @respx.mock
    async def test_authorization_query_param_also_rejected(self, rsa_keypair):
        mw = self._mw(_build_resource_server(rsa_keypair))
        status, _, _ = await _drive(mw, query_string=b"authorization=Bearer+abc")
        assert status == 400

    @respx.mock
    async def test_benign_query_value_containing_access_token_is_allowed(self, rsa_keypair):
        """Regression: the earlier substring-match implementation would
        false-positive on a value that contained ``access_token`` (e.g. a
        search filter); the parse_qs-based check only looks at parameter
        NAMES so benign values pass through."""
        mw = self._mw(_build_resource_server(rsa_keypair))
        token = _mint(
            rsa_keypair["private_pem"],
            aud="https://looker.example.com/mcp",
            iss="https://as.example.com",
        )
        status, _, _ = await _drive(
            mw,
            headers={"Authorization": f"Bearer {token}"},
            query_string=b"filter=access_token%3Dsome-value-in-a-value",
        )
        assert status == 200, "value-side occurrence of 'access_token' must not trigger rejection"

    @respx.mock
    async def test_well_known_path_bypasses_auth(self, rsa_keypair):
        mw = self._mw(_build_resource_server(rsa_keypair))
        status, _, _ = await _drive(mw, path="/.well-known/oauth-protected-resource")
        assert status == 200

    @respx.mock
    async def test_healthz_bypasses_auth(self, rsa_keypair):
        mw = self._mw(_build_resource_server(rsa_keypair))
        status, _, _ = await _drive(mw, path="/healthz")
        assert status == 200

    @respx.mock
    async def test_readyz_bypasses_auth(self, rsa_keypair):
        mw = self._mw(_build_resource_server(rsa_keypair))
        status, _, _ = await _drive(mw, path="/readyz")
        assert status == 200

    @respx.mock
    async def test_healthz_prefix_lookalike_does_not_bypass(self, rsa_keypair):
        """Regression: the earlier ``path.startswith(p)`` check would
        false-positive on ``/healthzfoo`` against the ``/healthz`` bypass
        entry. The fix requires either an exact match or a ``/`` path
        separator after the prefix."""
        mw = self._mw(_build_resource_server(rsa_keypair))
        status, _, _ = await _drive(mw, path="/healthzfoo")
        assert status == 401, "lookalike path must not bypass token validation"

    @respx.mock
    async def test_well_known_path_with_bearer_in_query_still_rejected(self, rsa_keypair):
        """Bearer-in-query is a protocol violation regardless of path — URL
        tokens leak into referrer headers and proxy logs."""
        mw = self._mw(_build_resource_server(rsa_keypair))
        status, _, _ = await _drive(
            mw,
            path="/.well-known/oauth-protected-resource",
            query_string=b"access_token=x",
        )
        assert status == 400

    @respx.mock
    async def test_empty_realm_rejected_at_construct(self, rsa_keypair):
        rs = _build_resource_server(rsa_keypair)
        with pytest.raises(ValueError, match="realm"):
            PublicModeAuthMiddleware(
                _echo_app,
                resource_server=rs,
                realm="",
                prm_url="https://x.example/md",
            )

    @respx.mock
    async def test_websocket_scope_passes_through(self, rsa_keypair):
        """Non-HTTP scopes (websocket, lifespan) pass through untouched.

        Verified by asserting the middleware does not emit an HTTP response
        of its own — a downstream app may or may not participate, but the
        auth gate specifically must not inject a 401/400.
        """
        mw = self._mw(_build_resource_server(rsa_keypair))
        received: list[dict] = []

        async def recv():
            return {"type": "lifespan.startup"}

        async def send(msg):
            received.append(msg)

        await mw({"type": "lifespan"}, recv, send)
        # Middleware must not short-circuit a non-HTTP scope with an auth
        # rejection — it has to delegate to the inner app.
        statuses = [m.get("status") for m in received if m.get("type") == "http.response.start"]
        assert 401 not in statuses
        assert 400 not in statuses
        assert any(m["type"].startswith("http.") for m in received)
