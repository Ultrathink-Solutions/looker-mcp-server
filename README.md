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
| **modeling** | `list_projects`, `get_project`, `create_project`, `update_project`, `delete_project`, `get_project_manifest`, `get_project_deploy_key`, `create_project_deploy_key`, `list_project_files`, `get_file`, `create_file`, `update_file`, `delete_file`, `validate_project`, `list_datagroups`, `get_datagroup`, `reset_datagroup`, `trigger_datagroup`, `start_pdt_build`, `check_pdt_build`, `stop_pdt_build`, `graph_derived_tables_for_view`, `graph_derived_tables_for_model` | LookML project lifecycle, file edits, syntax validation, datagroup cache + trigger management, and PDT build administration |
| **git** | `get_git_branch`, `list_git_branches`, `get_git_branch_by_name`, `create_git_branch`, `switch_git_branch`, `delete_git_branch`, `deploy_to_production`, `reset_to_production`, `get_git_deploy_key`, `create_git_deploy_key`, `list_git_connection_tests`, `run_git_connection_test` | Git branch lifecycle, production deploy, SSH deploy-key rotation, and git-connection diagnostics |
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

### Per-Call Admin Impersonation (`act_as_user`)

Looker dev mode (`workspace_id=dev`) is **per-user-isolated by design**. Each user has their own dev workspace; uncommitted LookML changes, the active branch, and dev-mode local branches all live in the calling user's workspace. That means an admin running `delete_git_branch` against the *admin's* dev workspace does nothing about a stuck branch in *another user's* dev workspace.

The git tools accept an optional `act_as_user` argument so an admin can perform the call as a different user — typically to clean up someone else's stuck dev-workspace state without leaving the MCP for raw HTTP. Accepts either a numeric user ID or an email address (resolved to an ID via Looker's user-search API).

```jsonc
// Example: admin sweeping a stale CI branch out of ci-bot's dev workspace
{
  "tool": "delete_git_branch",
  "arguments": {
    "project_id": "acme_analytics",
    "branch_name": "tmp_ci_5bd8888773",
    "act_as_user": "ci-bot@example.com"
  }
}
```

**Configuration.** Per-call admin impersonation is gated by `LOOKER_SUDO_AS_USER` — that flag is the single kill switch for sudo-capable behavior in the OSS server, and `act_as_user` respects it. Set `LOOKER_SUDO_AS_USER=true` (the default when admin credentials are configured) to enable. With `LOOKER_SUDO_AS_USER=false`, passing `act_as_user` raises a clear validation error rather than silently running the call under the configured identity — surfacing the misconfiguration at the call site instead of letting it route to the wrong user.

**Security model.** The MCP forwards capability — it does not gate it. Sudo permission is enforced by Looker server-side: if the configured `LOOKER_CLIENT_ID` does not have sudo capability, `login_user` returns HTTP 403 and the tool fails. There is no MCP-side "who may impersonate whom" policy in the open-source server; layer one in via a wrapping `IdentityProvider` if you need it (see the next section).

**Tool coverage.** All eight git/workspace-scoped tools accept `act_as_user`: `get_git_branch`, `list_git_branches`, `get_git_branch_by_name`, `create_git_branch`, `switch_git_branch`, `delete_git_branch`, `deploy_to_production`, `reset_to_production`. The four query tools accept it too — `query`, `query_sql`, `query_url`, `run_look` — for the CI pattern where queries against a feature branch must run under a dedicated service user's dev workspace rather than the calling admin's. Project-level tools (deploy keys, connection diagnostics) deliberately do not — they don't depend on per-user dev workspace state.

**Audit log.** Every argument-driven sudo emits an INFO-level structlog line:

```json
{
  "event": "looker.audit.act_as_user",
  "tool": "delete_git_branch",
  "target_user_id": "77",
  "target_user_email": "ci-bot@example.com",
  "triggered_by": "argument",
  "configured_user": "admin-api3-client-id"
}
```

This is independent of the trace-level `looker.session.sudo` debug line and is the right hook for downstream audit pipelines. Header-driven sudo (gateway pattern) is tagged `triggered_by="header"` on the debug line — `looker.audit.act_as_user` fires only for explicit per-call admin impersonation.

**Mode interaction.** `act_as_user` overrides the inner identity, including OAuth and header-based sudo. This is intentional — an explicit admin override should win over implicit gateway routing — but the underlying credentials must still have sudo capability, which Looker enforces. On Google Cloud core only Embed-type users can be impersonated; for regular GCC users use Mode 3 (OAuth pass-through) instead.

**Failure modes.**

- `act_as_user` is neither all-digits nor an email (no `@`) → validation error rejected up front, before any Looker call. Avoids forwarding garbage to `/login/{value}` where it would surface as an opaque HTTP 400.
- Email does not match any Looker user → validation error. Fail-loud is deliberate; silently falling back to the configured identity would let a typo'd email run the action under the wrong user.
- `LOOKER_SUDO_AS_USER=false` and `act_as_user` is passed → validation error explaining how to fix (enable sudo or remove the argument).
- Configured credentials lack sudo capability → Looker returns 403 on `login_user`, surfaced as `Permission denied — the current user lacks access.`

## Dev Mode and Branch Validation

The query tools (`query`, `query_sql`, `query_url`, `run_look`) and the modeling/git tools accept three optional arguments — `dev_mode`, `branch`, and `project_id` — that together let you run operations against the LookML in a Looker dev workspace rather than production. This is what makes feature-branch validation possible from the MCP without falling back to raw REST.

**How Looker scopes workspaces.** Workspace selection (production vs. dev) is a property of the API session token, not the call. The MCP issues `PATCH /session {"workspace_id": "dev"}` immediately after authentication when `dev_mode=True` is set; this affects every subsequent call routed through the same session. The setting does not persist across logins, so each MCP call sets it explicitly.

**Branch state is per-Looker-user, server-side.** Each Looker user has exactly one dev workspace, with one currently-checked-out branch per LookML project. The branch checkout persists across logouts and concurrent calls — it's mutable shared state on Looker's server. Two operations against the same user fight over this single cell.

### Atomic branch swap

Set `branch="<feature-branch>"` and `project_id="<lookml-project>"` on a query tool to atomically:

1. Save the user's currently-checked-out branch on the project.
2. PUT the target branch.
3. Run the query.
4. Restore the saved branch in `finally` (even if the query raises).

`branch` implies `dev_mode=True`. The save and restore are no-ops when the dev workspace is already on the target branch.

### Canonical workflows

**One-shot CI: validate a PR's LookML against real data.** Single tool call, atomic. The dedicated CI service user's dev workspace is borrowed for the duration; the saved branch is restored before the call returns.

```jsonc
{
  "tool": "query",
  "arguments": {
    "model": "ecommerce",
    "view": "orders",
    "fields": ["orders.region", "orders.total_revenue"],
    "branch": "feature/new-aggregation",
    "project_id": "ecommerce",
    "act_as_user": "ci-bot@example.com"
  }
}
```

**Production vs. PR comparison.** Two calls — the LLM diffs the results in its own context.

```jsonc
{ "tool": "query", "arguments": { "model": "ecommerce", "view": "orders", "fields": [...] } }
{ "tool": "query", "arguments": { "model": "ecommerce", "view": "orders", "fields": [...],
                                   "branch": "feature/new-aggregation", "project_id": "ecommerce",
                                   "act_as_user": "ci-bot@example.com" } }
```

**Iterative human debug.** The branch state is sticky in the dev workspace, so set it once with `switch_git_branch` and run multiple queries with `dev_mode=True` (no `branch` arg). Restore the user's normal branch with another `switch_git_branch` when done.

```jsonc
{ "tool": "switch_git_branch", "arguments": { "project_id": "ecommerce", "branch_name": "feature/new-aggregation" } }
{ "tool": "query",             "arguments": { "model": "ecommerce", "view": "orders", "fields": [...], "dev_mode": true } }
{ "tool": "update_lookml_file", "arguments": { ... } }
{ "tool": "query",             "arguments": { "model": "ecommerce", "view": "orders", "fields": [...], "dev_mode": true } }
{ "tool": "switch_git_branch", "arguments": { "project_id": "ecommerce", "branch_name": "main" } }
```

**Cleanup another user's stuck dev workspace.** Combine `act_as_user` with the git tools to operate on someone else's per-user state.

```jsonc
{
  "tool": "switch_git_branch",
  "arguments": { "project_id": "ecommerce", "branch_name": "main", "act_as_user": "alice@example.com" }
}
```

### Concurrency caveat

Looker's per-user-per-project branch checkout is a single mutable cell. Two concurrent operations on the same `act_as_user` (or the same configured admin identity, when `act_as_user` is omitted) race on it. The atomic save+restore prevents accidental state leaks, but it does **not** serialize concurrent calls — if your CI fans out across many open PRs against a single ci-bot user, you'll see non-deterministic results.

For parallel PR validation, provision multiple Looker users (e.g. `ci-bot-1`, `ci-bot-2`, …) and have your CI fan-out logic rotate through them via `act_as_user`. There is no MCP-side mutex; this is an operational choice the deployer makes.

### What `dev_mode` does *not* cover (v1)

- **Multi-project manifest imports.** If your LookML project imports another project, the import stays on whatever branch is currently checked out in the dev workspace for that imported project. The atomic swap is single-project; recursive manifest-aware swapping is a v2 concern.
- **Cross-call session continuity.** Each tool call gets its own ephemeral API session (login → operation → logout), so `dev_mode=True` only takes effect within a single call. The branch state persists across calls because Looker stores it server-side per-user; the workspace setting does not.

### Coverage by tool group

`dev_mode`, `branch`, and `act_as_user` are propagated through the tool groups that work with workspace-scoped LookML state. Tools that read workspace-agnostic metadata don't accept these args.

| Tool group | Workspace-aware tools | Production-only tools |
|---|---|---|
| **git** | `switch_git_branch`, `create_git_branch`, `delete_git_branch`, `reset_to_production` (default `dev_mode=True`) | `get_git_branch`, `list_git_branches`, `get_git_branch_by_name`, `deploy_to_production` (read prod git state) |
| **query** | `query`, `query_sql`, `query_url`, `run_look` (default `dev_mode=False`; opt in via `branch=` or `dev_mode=True`) | `run_dashboard`, `search_content` (production content) |
| **modeling — file ops** | `list_project_files`, `get_file` (default `dev_mode=True`), `create_file`, `update_file`, `delete_file` (always dev — Looker rejects writes to production) | — |
| **modeling — validation** | `validate_project` (default `dev_mode=False`; opt in via `branch=` for PR validation) | — |
| **modeling — data tests** | `list_lookml_tests`, `run_lookml_tests` (default `dev_mode=False`; opt in via `branch=` for PR data-regression checks) | — |
| **modeling — project metadata** | — | `list_projects`, `get_project`, `get_project_manifest`, `list_datagroups`, `reset_datagroup` (workspace-agnostic project state) |

### `run_lookml_tests` — PR data-regression checks

`run_lookml_tests(project_id="ecommerce", branch="feature-x", act_as_user="ci-bot@example.com")` is the primary primitive for catching data-regression bugs introduced by a PR. Looker compiles each test's `explore_source` query, runs it against the warehouse, and evaluates the assertion expression against the result rows. Failures come back with assertion-level detail (`model_name`, `test_name`, `errors[]`).

Default per-call timeout is 1800s (30 min) because data tests run real warehouse queries with assertions and can take a long time on large tables — same default Spectacles uses.

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

## PDT Administration Workflows

PDT (Persistent Derived Table) lifecycle is split across two tool groups: the `connection` group's `update_connection` toggles PDT control on a connection and the `modeling` group's `start_pdt_build` / `check_pdt_build` / `stop_pdt_build` (build management), `trigger_datagroup` (force rebuild + cache invalidation), and `graph_derived_tables_for_*` (dependency inspection) cover the per-PDT operations.

Two opinionated recipes for connection-level workflows:

### Disable PDT workflow on a connection

When you need to quiesce all PDT builds on a connection (warehouse maintenance, cost spike investigation, etc.):

```jsonc
// 1. Stop new builds at the source — Looker will reject any further enqueues
{ "tool": "update_connection", "args": { "name": "my_warehouse", "pdt_api_control_enabled": false } }

// 2. Inspect what's currently materialized so you know what's at risk
{ "tool": "graph_derived_tables_for_model", "args": { "model": "ecommerce", "color": true } }

// 3. (Optional) Stop any in-flight builds you have materialization_ids for
{ "tool": "stop_pdt_build", "args": { "materialization_id": "mat-abc" } }

// 4. Verify the connection is quiesced
{ "tool": "test_connection", "args": { "name": "my_warehouse", "tests": ["pdt"] } }
```

### Enable PDT workflow on a connection

When you're ready to re-enable PDT builds after maintenance:

```jsonc
// 1. Re-enable PDT API control
{ "tool": "update_connection", "args": { "name": "my_warehouse", "pdt_api_control_enabled": true } }

// 2. Verify the connection is healthy for PDT builds
{ "tool": "test_connection", "args": { "name": "my_warehouse", "tests": ["pdt"] } }

// 3. (Optional) Force-rebuild gating datagroups so downstream PDTs catch up
{ "tool": "trigger_datagroup", "args": { "datagroup_id": "dg1" } }

// 4. (Optional) Pre-warm specific PDTs
{ "tool": "start_pdt_build", "args": { "model_name": "ecommerce", "view_name": "orders_pdt" } }
{ "tool": "check_pdt_build", "args": { "materialization_id": "mat-…" } }  // poll until status == "complete"
```

These recipes are intentionally exposed as separate primitives rather than a single `disable_pdt_workflow(connection)` composite tool. Each call emits its own audit line in the `looker.session.sudo` debug log when run under `act_as_user`, which is the right granularity for compliance review. A composite tool would hide steps from the LLM-as-operator and make failure paths less legible.

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
