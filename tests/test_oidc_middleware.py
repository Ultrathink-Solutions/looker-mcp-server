"""Tests for :class:`PublicModeAuthMiddleware` — the ASGI auth gate."""

from __future__ import annotations

import json
import time

import httpx
import jwt as pyjwt
import pytest
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


@pytest.fixture
def resource_server(rsa_keypair, respx_mock):
    cache = JWKSCache("https://as.example.com/.well-known/jwks.json")
    respx_mock.get(cache.jwks_uri).mock(
        return_value=httpx.Response(200, json=_jwks_body(rsa_keypair["public_key"]))
    )
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

    async def test_valid_token_passes_through(self, rsa_keypair, resource_server):
        mw = self._mw(resource_server)
        token = _mint(
            rsa_keypair["private_pem"],
            aud="https://looker.example.com/mcp",
            iss="https://as.example.com",
        )
        status, _, body = await _drive(mw, headers={"Authorization": f"Bearer {token}"})
        assert status == 200
        assert json.loads(body) == {"authenticated_sub": "user-1"}

    async def test_missing_authorization_401_with_challenge(self, resource_server):
        mw = self._mw(resource_server)
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

    async def test_malformed_bearer_prefix_401(self, resource_server):
        mw = self._mw(resource_server)
        status, _, _ = await _drive(mw, headers={"Authorization": "Basic abcd"})
        assert status == 401

    async def test_invalid_token_401(self, rsa_keypair, resource_server):
        """Token signed with a foreign key fails signature verification."""
        mw = self._mw(resource_server)
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

    async def test_bearer_in_query_rejected_with_400(self, resource_server):
        mw = self._mw(resource_server)
        status, headers, body = await _drive(mw, query_string=b"access_token=attacker")
        assert status == 400
        assert headers.get("content-type") == "application/json"
        parsed = json.loads(body)
        assert parsed["error"] == "invalid_request"
        assert "OAuth 2.1" in parsed["error_description"]

    async def test_authorization_query_param_also_rejected(self, resource_server):
        mw = self._mw(resource_server)
        status, _, _ = await _drive(mw, query_string=b"authorization=Bearer+abc")
        assert status == 400

    async def test_well_known_path_bypasses_auth(self, resource_server):
        mw = self._mw(resource_server)
        status, _, _ = await _drive(mw, path="/.well-known/oauth-protected-resource")
        assert status == 200

    async def test_healthz_bypasses_auth(self, resource_server):
        mw = self._mw(resource_server)
        status, _, _ = await _drive(mw, path="/healthz")
        assert status == 200

    async def test_readyz_bypasses_auth(self, resource_server):
        mw = self._mw(resource_server)
        status, _, _ = await _drive(mw, path="/readyz")
        assert status == 200

    async def test_well_known_path_with_bearer_in_query_still_rejected(self, resource_server):
        """Bearer-in-query is a protocol violation regardless of path — URL
        tokens leak into referrer headers and proxy logs."""
        mw = self._mw(resource_server)
        status, _, _ = await _drive(
            mw,
            path="/.well-known/oauth-protected-resource",
            query_string=b"access_token=x",
        )
        assert status == 400

    async def test_empty_realm_rejected_at_construct(self, resource_server):
        with pytest.raises(ValueError, match="realm"):
            PublicModeAuthMiddleware(
                _echo_app,
                resource_server=resource_server,
                realm="",
                prm_url="https://x.example/md",
            )

    async def test_websocket_scope_passes_through(self, resource_server):
        """Non-HTTP scopes (websocket, lifespan) pass through untouched."""
        mw = self._mw(resource_server)
        received = []

        async def recv():
            return {"type": "lifespan.startup"}

        async def send(msg):
            received.append(msg)

        await mw({"type": "lifespan"}, recv, send)
        # No ASGI send from our middleware; _echo_app emits http-shaped
        # messages regardless, which we accept as "passed through".
        assert any(m["type"].startswith("http.") for m in received)
