# looker-mcp-server

[![PyPI - Version](https://img.shields.io/pypi/v/looker-mcp-server?style=flat-square)](https://pypi.org/project/looker-mcp-server/)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/looker-mcp-server?style=flat-square)](https://pypi.org/project/looker-mcp-server/)
[![License](https://img.shields.io/github/license/ultrathink-solutions/looker-mcp-server?style=flat-square)](LICENSE)
[![CI](https://img.shields.io/github/actions/workflow/status/ultrathink-solutions/looker-mcp-server/ci.yml?branch=main&style=flat-square&label=CI)](https://github.com/ultrathink-solutions/looker-mcp-server/actions/workflows/ci.yml)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json&style=flat-square)](https://github.com/astral-sh/ruff)

A full-featured [Model Context Protocol](https://modelcontextprotocol.io) (MCP) server for the [Looker API](https://cloud.google.com/looker/docs/reference/looker-api/latest). Gives AI assistants direct access to your Looker instance — querying the semantic model, managing content, editing LookML, and administering users — all through a standard MCP interface.

## Features

- **160 tools** across 15 groups covering the full Looker API surface
- **Semantic layer queries** — query through LookML models, not raw SQL
- **OAuth pass-through** — forward user tokens from an upstream gateway or MCP OAuth flow
- **User impersonation** — admin sudo on self-hosted Looker, OAuth on Google Cloud core
- **Dual transport** — stdio for local/CLI use, streamable-http for production deployment
- **Selective tool loading** — enable only the tool groups you need via `--groups`
- **Pluggable identity** — swap in custom authentication via the `IdentityProvider` protocol
- **Health endpoints** — `/healthz` and `/readyz` for container orchestration

## Quick Start

### Installation

```bash
pip install looker-mcp-server
# or
uv add looker-mcp-server
```

### Environment Variables

At minimum, set your Looker instance URL and API3 credentials:

```bash
export LOOKER_BASE_URL="https://mycompany.looker.com"
export LOOKER_CLIENT_ID="your-api3-client-id"
export LOOKER_CLIENT_SECRET="your-api3-client-secret"
```

### Run with stdio (for Claude Code, Claude Desktop, etc.)

```bash
looker-mcp-server --groups explore,query,schema
```

### Run with HTTP (for production deployment)

```bash
LOOKER_TRANSPORT=streamable-http looker-mcp-server --groups all --port 8080
```

## MCP Client Configuration

### Claude Code

Add to your Claude Code MCP settings:

```json
{
  "mcpServers": {
    "looker": {
      "command": "looker-mcp-server",
      "args": ["--groups", "explore,query,schema,content"],
      "env": {
        "LOOKER_BASE_URL": "https://mycompany.looker.com",
        "LOOKER_CLIENT_ID": "your-client-id",
        "LOOKER_CLIENT_SECRET": "your-client-secret"
      }
    }
  }
}
```

### Claude Desktop

Add to `~/Library/Application Support/Claude/claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "looker": {
      "command": "looker-mcp-server",
      "args": ["--groups", "explore,query,schema,content"],
      "env": {
        "LOOKER_BASE_URL": "https://mycompany.looker.com",
        "LOOKER_CLIENT_ID": "your-client-id",
        "LOOKER_CLIENT_SECRET": "your-client-secret"
      }
    }
  }
}
```

## Tool Groups

Tools are organized into groups that can be selectively enabled. Default groups are marked with **\***.

| Group | Tools | Description |
|-------|-------|-------------|
| **explore**\* | `list_models`, `get_model`, `get_explore`, `list_dimensions`, `list_measures`, `list_connections` | Browse LookML models, explores, and fields |
| **query**\* | `query`, `query_sql`, `run_look`, `run_dashboard`, `query_url`, `search_content` | Run queries through the semantic layer |
| **schema**\* | `list_databases`, `list_schemas`, `list_tables`, `list_columns` | Inspect underlying database schema |
| **content**\* | `list_looks`, `create_look`, `update_look`, `delete_look`, `list_dashboards`, `create_dashboard`, `update_dashboard`, `delete_dashboard`, `add_dashboard_element`, `add_dashboard_filter`, `generate_embed_url`, `validate_content` | Manage Looks and dashboards |
| **board** | `list_boards`, `get_board`, `create_board`, `update_board`, `delete_board`, `get_board_section`, `create_board_section`, `update_board_section`, `delete_board_section`, `get_board_item`, `create_board_item`, `update_board_item`, `delete_board_item` | Curate content with boards, sections, and items |
| **folder** | `list_folders`, `get_folder`, `create_folder`, `update_folder`, `delete_folder`, `get_folder_children`, `get_folder_ancestors`, `get_folder_looks`, `get_folder_dashboards` | Navigate and manage the folder hierarchy |
| **health**\* | `health_pulse`, `health_analyze`, `health_vacuum` | Instance health checks and usage analysis |
| **modeling** | `list_projects`, `get_project`, `create_project`, `update_project`, `delete_project`, `get_project_manifest`, `get_project_deploy_key`, `create_project_deploy_key`, `list_project_files`, `get_file`, `create_file`, `update_file`, `delete_file`, `validate_project`, `list_datagroups`, `reset_datagroup` | LookML project lifecycle, file edits, syntax validation, and datagroup cache management |
| **git** | `get_git_branch`, `list_git_branches`, `create_git_branch`, `switch_git_branch`, `deploy_to_production`, `reset_to_production` | Git operations and production deployment |
| **admin** | `list_users`, `get_user`, `create_user`, `update_user`, `delete_user`, `create_credentials_email`, `send_password_reset`, `list_roles`, `get_role`, `create_role`, `update_role`, `delete_role`, `get_role_groups`, `get_role_users`, `list_permissions`, `list_permission_sets`, `create_permission_set`, `update_permission_set`, `delete_permission_set`, `list_model_sets`, `create_model_set`, `update_model_set`, `delete_model_set`, `list_groups`, `create_group`, `delete_group`, `add_group_user`, `remove_group_user`, `set_role_groups`, `set_role_users`, `set_user_roles`, `get_user_roles`, `list_schedules`, `create_schedule`, `update_schedule`, `delete_schedule`, `run_schedule_once` | User, role, RBAC, group, and schedule management |
| **connection** | `get_connection`, `list_connection_dialects`, `create_connection`, `update_connection`, `delete_connection`, `test_connection` | Database connection CRUD and health checks |
| **user_attributes** | `list_user_attributes`, `get_user_attribute`, `create_user_attribute`, `update_user_attribute`, `delete_user_attribute`, `list_user_attribute_group_values`, `set_user_attribute_group_values`, `delete_user_attribute_group_value`, `list_user_attribute_values_for_user`, `set_user_attribute_user_value`, `delete_user_attribute_user_value` | User attribute definitions plus per-group and per-user value overrides (row-level security, per-developer credentials, filter defaults) |
| **credentials** | `list_credentials_api3`, `create_credentials_api3`, `get_credentials_api3`, `delete_credentials_api3`, `get_credentials_ldap`, `delete_credentials_ldap`, `get_credentials_saml`, `delete_credentials_saml`, `get_credentials_oidc`, `delete_credentials_oidc`, `get_credentials_google`, `delete_credentials_google` | Non-email credentials — API3 key-pair rotation plus get/delete for LDAP, SAML, OIDC, and Google SSO links |
| **audit** | `get_query_history`, `get_content_usage`, `get_pdt_build_log`, `get_schedule_history`, `get_user_activity_log`, `list_running_queries`, `kill_query`, `list_active_sessions`, `get_session`, `terminate_session`, `list_project_ci_runs`, `get_project_ci_run`, `trigger_project_ci_run` | Query history, content usage, PDT build + schedule + event logs via system__activity, plus live-ops (running queries, sessions, CI runs) |
| **workflows** | `provision_connection`, `bootstrap_lookml_project`, `deploy_lookml_changes`, `rollback_to_production`, `provision_user`, `grant_access`, `offboard_user`, `rotate_api_credentials`, `audit_query_activity`, `audit_instance_health`, `investigate_runaway_queries`, `find_stale_content`, `disable_stale_sessions` | Task-oriented Layer 2 compositions — provisioning workflows (bootstrap, deploy, provision users) plus ops/audit workflows (offboard, rotate credentials, audit, cleanup) |

### Selecting Groups

```bash
# Default groups only (explore, query, schema, content, health)
looker-mcp-server

# Specific groups
looker-mcp-server --groups explore,query

# All groups (including board, folder, modeling, git, admin, connection, user_attributes, credentials, audit, workflows)
looker-mcp-server --groups all
```

## Configuration Reference

All settings are configured via environment variables with the `LOOKER_` prefix, or via a `.env` file.

| Variable | Default | Description |
|----------|---------|-------------|
| `LOOKER_BASE_URL` | *(required)* | Base URL of the Looker instance |
| `LOOKER_CLIENT_ID` | | API3 client ID for service account |
| `LOOKER_CLIENT_SECRET` | | API3 client secret for service account |
| `LOOKER_API_VERSION` | `4.0` | Looker API version |
| `LOOKER_DEPLOYMENT_TYPE` | `self_hosted` | `self_hosted` or `google_cloud_core` |
| `LOOKER_TRANSPORT` | `stdio` | `stdio` or `streamable-http` |
| `LOOKER_HOST` | `0.0.0.0` | HTTP bind address |
| `LOOKER_PORT` | `8080` | HTTP port |
| `LOOKER_SUDO_AS_USER` | `true` | Enable user impersonation when identity headers are present |
| `LOOKER_SUDO_ASSOCIATIVE` | `false` | Attribute sudo activity to admin (`true`) or impersonated user (`false`) |
| `LOOKER_USER_EMAIL_HEADER` | `X-User-Email` | HTTP header carrying user email for sudo impersonation |
| `LOOKER_USER_TOKEN_HEADER` | `X-User-Token` | HTTP header carrying pre-exchanged OAuth token |
| `LOOKER_TIMEOUT` | `60.0` | HTTP request timeout in seconds |
| `LOOKER_MAX_ROWS` | `5000` | Default maximum rows for query tools |
| `LOOKER_VERIFY_SSL` | `true` | Verify TLS certificates |
| `LOOKER_LOG_LEVEL` | `INFO` | Logging level |
| `LOOKER_MCP_MODE` | `dev` | `dev` (permissive) or `public` (OAuth 2.1 resource-server, MCP 2025-11-25). See [MCP-Level Authentication](#mcp-level-authentication). |
| `LOOKER_MCP_JWKS_URI` | | Authorization server JWK Set URL (RFC 7517). **Required when `LOOKER_MCP_MODE=public`.** Must be an `https://` URL. |
| `LOOKER_MCP_ISSUER_URL` | | Expected `iss` claim (RFC 8414). **Required when `LOOKER_MCP_MODE=public`.** Must be an `https://` URL. |
| `LOOKER_MCP_RESOURCE_URI` | | This server's canonical URI for RFC 8707 audience binding and the RFC 9728 PRM `resource` field. **Required when `LOOKER_MCP_MODE=public`.** Must be an `https://` URL without fragment. |
| `LOOKER_MCP_AUTH_TOKEN` | | Static bearer token for MCP-level authentication. **Deprecated** — emits a warning in `dev` mode, rejected outright in `public` mode (RFC 9068 §2.1 forbids symmetric static bearers for OAuth 2.1 access tokens). Scheduled for removal in a future major release; migrate to `LOOKER_MCP_MODE=public`. |

## Authentication & Impersonation

The server supports three authentication modes, selected automatically based on configuration and request headers.

### Mode 1: Service Account (API Key)

The simplest mode — all API calls use the configured service-account credentials.

```bash
export LOOKER_CLIENT_ID="your-api3-client-id"
export LOOKER_CLIENT_SECRET="your-api3-client-secret"
export LOOKER_SUDO_AS_USER=false
```

### Mode 2: Admin Sudo (Self-Hosted Looker)

An admin service account impersonates individual users via Looker's `login_user` API. The user is identified by an email address in the request headers (typically set by an upstream gateway).

```bash
export LOOKER_CLIENT_ID="admin-api3-client-id"
export LOOKER_CLIENT_SECRET="admin-api3-client-secret"
export LOOKER_DEPLOYMENT_TYPE=self_hosted
export LOOKER_SUDO_AS_USER=true
```

When a request arrives with `X-User-Email: alice@company.com`, the server:
1. Logs in with admin credentials
2. Looks up Alice's Looker user ID by email
3. Creates a sudo session as Alice via `login_user`
4. Executes the tool call as Alice
5. Logs out both sessions

> **Note:** On Looker (Google Cloud core), `login_user` only works for Embed-type users. Regular users require OAuth mode.

### Mode 3: OAuth Pass-Through (Google Cloud Core)

For Looker (Google Cloud core) deployments where regular users cannot be impersonated via sudo. An upstream gateway performs OAuth token exchange and passes the user's token in a header.

```bash
export LOOKER_CLIENT_ID="fallback-api3-client-id"
export LOOKER_CLIENT_SECRET="fallback-api3-client-secret"
export LOOKER_DEPLOYMENT_TYPE=google_cloud_core
export LOOKER_SUDO_AS_USER=true
```

When a request arrives with `X-User-Token: <oauth-access-token>`, the server uses that token directly — no login/logout cycle needed.

If no token header is present, the server falls back to service-account mode.

### Automatic Mode Selection

When `LOOKER_SUDO_AS_USER=true` (the default), the server uses a `DualModeIdentityProvider` that automatically routes:

- **Self-hosted** → sudo (via `X-User-Email` header)
- **Google Cloud core** → OAuth (via `X-User-Token` header)
- **No identity headers** → service account fallback

## Extending with Custom Identity Providers

The `IdentityProvider` protocol is the primary extension point for integrating with custom authentication systems.

```python
from looker_mcp_server.identity import IdentityProvider, LookerIdentity, RequestContext
from looker_mcp_server.server import create_server
from looker_mcp_server.config import LookerConfig


class MyIdentityProvider:
    """Custom identity provider that integrates with your auth system."""

    async def resolve(self, context: RequestContext) -> LookerIdentity:
        # Extract identity from headers, tokens, etc.
        token = context.headers.get("authorization", "").removeprefix("Bearer ")

        if token:
            # Exchange for a Looker-scoped token via your auth system
            looker_token = await my_token_exchange(token)
            return LookerIdentity(mode="oauth", access_token=looker_token)

        # Fall back to service account
        return LookerIdentity(
            mode="api_key",
            client_id="your-client-id",
            client_secret="your-client-secret",
        )


# Wire it up
config = LookerConfig()
mcp, client = create_server(config, identity_provider=MyIdentityProvider())
```

The `RequestContext` provides:
- `headers` — HTTP request headers (empty in stdio mode)
- `tool_name` — name of the MCP tool being invoked
- `tool_group` — which group the tool belongs to
- `arguments` — arguments passed to the tool

## MCP-Level Authentication

MCP-level authentication (who can connect to the server) has two modes, selected by `LOOKER_MCP_MODE`.

### `LOOKER_MCP_MODE=dev` (default) — permissive

Intended for local development, stdio deployments, and trust-network scenarios behind an upstream gateway. Two sub-options:

1. **No MCP-level auth** (default) — any client that can reach the transport can connect.
2. **Static bearer token** (deprecated) — set `LOOKER_MCP_AUTH_TOKEN` and clients must present it. Emits a `DeprecationWarning` at startup because RFC 9068 §2.1 forbids symmetric static bearers for OAuth 2.1 access tokens, and because static bearers don't carry per-user identity or expiry. Scheduled for removal in a future major release — migrate to `LOOKER_MCP_MODE=public`.

### `LOOKER_MCP_MODE=public` — OAuth 2.1 resource-server (MCP 2025-11-25)

Internet-exposed / compliance-gated deployments. The server:

- Validates every request's `Authorization: Bearer <JWT>` header as an OAuth 2.1 access token.
- Accepts only `RS256` and `ES256` signatures (RFC 9068 §2.1). HS256 is hard-rejected at header inspection to close the algorithm-confusion attack vector (CVE-2015-9235).
- Caches the authorization server's JWKS (RFC 7517) with a 1-hour TTL and throttled kid-miss refresh (≤1 forced refresh per 5 minutes).
- Enforces `iss` (RFC 8414) and `aud` (RFC 8707) claim binding.
- Serves an RFC 9728 Protected Resource Metadata document for client auto-discovery. The spec-canonical URL follows RFC 9728 §3 construction: `/.well-known/oauth-protected-resource` when `LOOKER_MCP_RESOURCE_URI` is an origin-only identifier, or `/.well-known/oauth-protected-resource<resource-path>` when it carries a path. The origin-rooted path is also served as a defensive fallback.
- Emits realm-bearing `WWW-Authenticate` challenges on 401 (RFC 7235 §4.1 + RFC 9728 §5.1) pointing clients at the PRM URL.
- Rejects URL-query bearer tokens (`?access_token=`, `?authorization=`) with a 400 `invalid_request` per OAuth 2.1 §5.1.1 — URL-bound tokens leak into referrer headers, proxy logs, and browser history regardless of destination.
- **Rejects `LOOKER_MCP_AUTH_TOKEN` outright** — if the static bearer env var is set alongside `LOOKER_MCP_MODE=public`, the server fails to start.

Required configuration:

```bash
export LOOKER_MCP_MODE=public
export LOOKER_MCP_JWKS_URI="https://auth.example.com/.well-known/jwks.json"
export LOOKER_MCP_ISSUER_URL="https://auth.example.com"
export LOOKER_MCP_RESOURCE_URI="https://looker-mcp.example.com/mcp"
```

All three URIs must be absolute `https://` URLs; the server fails closed at startup with a typed `DeploymentPostureError` if any are missing, malformed, or use `http://`. The `LOOKER_MCP_RESOURCE_URI` must not carry a fragment (RFC 9728 §3).

#### Deprecation timeline for `LOOKER_MCP_AUTH_TOKEN`

- **This release (0.13.0)** — deprecated in `dev` mode (warning emitted), rejected in `public` mode (startup failure).
- **Future major release** — removed entirely.

If you currently rely on `LOOKER_MCP_AUTH_TOKEN` for gateway-level MCP protection, plan the migration now: either stand up an authorization server that issues OAuth 2.1 access tokens bound to `aud=<LOOKER_MCP_RESOURCE_URI>`, or keep the server in `dev` mode behind a trusted network perimeter.

## Health Endpoints

When running in HTTP mode, the server exposes:

- `GET /healthz` — liveness probe (always returns 200 if server is running)
- `GET /readyz` — readiness probe (verifies Looker connectivity with a login/logout cycle)
- `GET /.well-known/oauth-protected-resource` — RFC 9728 Protected Resource Metadata (only when `LOOKER_MCP_MODE=public`). When `LOOKER_MCP_RESOURCE_URI` has a path, the same document is also served at `/.well-known/oauth-protected-resource<resource-path>` — that is the spec-canonical URL per RFC 9728 §3, and the one referenced by `resource_metadata=...` in 401 `WWW-Authenticate` challenges.

## Development

```bash
# Clone
git clone https://github.com/ultrathink-solutions/looker-mcp-server.git
cd looker-mcp-server

# Install dependencies
uv sync --locked --dev

# Run quality checks
uv run ruff check .        # lint
uv run ruff format .       # format
uv run pyright             # type check
uv run pytest tests/ -v    # tests
```

See [CONTRIBUTING.md](.github/CONTRIBUTING.md) for contribution guidelines.

## License

[Apache License 2.0](LICENSE)
