# looker-mcp-server

[![PyPI - Version](https://img.shields.io/pypi/v/looker-mcp-server?style=flat-square)](https://pypi.org/project/looker-mcp-server/)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/looker-mcp-server?style=flat-square)](https://pypi.org/project/looker-mcp-server/)
[![License](https://img.shields.io/github/license/ultrathink-solutions/looker-mcp-server?style=flat-square)](LICENSE)
[![CI](https://img.shields.io/github/actions/workflow/status/ultrathink-solutions/looker-mcp-server/ci.yml?branch=main&style=flat-square&label=CI)](https://github.com/ultrathink-solutions/looker-mcp-server/actions/workflows/ci.yml)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json&style=flat-square)](https://github.com/astral-sh/ruff)

A full-featured [Model Context Protocol](https://modelcontextprotocol.io) (MCP) server for the [Looker API](https://cloud.google.com/looker/docs/reference/looker-api/latest). Gives AI assistants direct access to your Looker instance — querying the semantic model, managing content, editing LookML, and administering users — all through a standard MCP interface.

## Features

- **56 tools** across 8 groups covering the full Looker API surface
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
| **content**\* | `list_looks`, `create_look`, `update_look`, `delete_look`, `list_dashboards`, `create_dashboard`, `update_dashboard`, `delete_dashboard`, `add_dashboard_element`, `add_dashboard_filter`, `generate_embed_url` | Manage Looks and dashboards |
| **health**\* | `health_pulse`, `health_analyze`, `health_vacuum` | Instance health checks and usage analysis |
| **modeling** | `list_projects`, `list_project_files`, `get_file`, `create_file`, `update_file`, `delete_file`, `validate_project` | Edit LookML files and validate syntax |
| **git** | `get_git_branch`, `list_git_branches`, `create_git_branch`, `switch_git_branch`, `deploy_to_production`, `reset_to_production` | Git operations and production deployment |
| **admin** | `list_users`, `get_user`, `create_user`, `update_user`, `delete_user`, `list_roles`, `create_role`, `list_groups`, `add_group_user`, `remove_group_user`, `list_schedules`, `create_schedule`, `delete_schedule` | User, role, group, and schedule management |

### Selecting Groups

```bash
# Default groups only (explore, query, schema, content, health)
looker-mcp-server

# Specific groups
looker-mcp-server --groups explore,query

# All groups (including modeling, git, admin)
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
| `LOOKER_MCP_AUTH_TOKEN` | | Static bearer token for MCP-level authentication |

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

To protect the MCP server itself (who can connect to it), set a static bearer token:

```bash
export LOOKER_MCP_AUTH_TOKEN="your-secret-mcp-token"
```

MCP clients must then include this token in their connection. This is separate from Looker API authentication.

## Health Endpoints

When running in HTTP mode, the server exposes:

- `GET /healthz` — liveness probe (always returns 200 if server is running)
- `GET /readyz` — readiness probe (verifies Looker connectivity with a login/logout cycle)

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
