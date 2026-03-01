# Contributing to looker-mcp-server

Thank you for your interest in contributing! This guide will help you get started.

## Before You Start

All non-trivial changes require an issue first. Please open one before writing code so we can discuss the approach. Maintainers may close PRs without a linked issue.

## Development Setup

**Requirements:** Python 3.11+, [uv](https://docs.astral.sh/uv/)

```bash
git clone https://github.com/ultrathink-solutions/looker-mcp-server.git
cd looker-mcp-server
uv sync --locked --dev
```

## Running Quality Checks

All four checks must pass before submitting a PR. CI enforces this.

```bash
uv run ruff format .       # format code
uv run ruff check .        # lint
uv run pyright             # type check
uv run pytest tests/ -v    # run tests
```

## Project Structure

```
src/looker_mcp_server/
├── config.py          # Pydantic settings (env vars)
├── identity.py        # IdentityProvider protocol + built-in providers
├── client.py          # Looker API client with ephemeral sessions
├── middleware.py       # ASGI header capture
├── server.py          # Server factory + tool group registry
├── main.py            # CLI entry point
└── tools/
    ├── explore.py     # Semantic model discovery
    ├── query.py       # Queries and content search
    ├── schema.py      # Database introspection
    ├── content.py     # Look and dashboard CRUD
    ├── modeling.py    # LookML file editing
    ├── git.py         # Git operations and deployment
    ├── admin.py       # User/role/group management
    └── health.py      # Instance health checks
```

## Adding a New Tool

1. Identify which tool group it belongs to (or propose a new group)
2. Add the tool function in the appropriate `tools/*.py` file
3. Follow the existing pattern: `@server.tool()` decorator, `client.build_context()`, `async with client.session(ctx) as session:`
4. Return JSON strings (not dicts) — this is required by MCP
5. Wrap the body in `try/except` and use `format_api_error()` for error handling
6. Add tests

## Adding a New Tool Group

1. Create `src/looker_mcp_server/tools/your_group.py` with a `register_your_group_tools(server, client)` function
2. Register it in `server.py`'s `_group_registry` dict
3. Add the group name to `ALL_GROUPS` in `config.py`
4. Decide if it should be in `DEFAULT_GROUPS` (read-only, safe groups) or opt-in only

## Code Style

- **Type hints** on all functions (Python 3.11+ syntax: `list[str]`, not `List[str]`)
- **Pydantic** for data models and settings (not dataclasses)
- **structlog** for logging
- **ruff** enforces formatting and linting rules
- **pyright** in standard mode for type checking
- Docstrings on public APIs (modules, classes, exported functions)

## Commit Messages

Follow [Conventional Commits](https://www.conventionalcommits.org/):

```
feat: add connection test tool to health group
fix: handle empty response from /lookml_models endpoint
docs: add OAuth setup guide to README
refactor: extract common query builder logic
test: add tests for DualModeIdentityProvider
chore: update fastmcp dependency to 2.15
```

## Pull Request Process

1. Fork the repo and create a branch from `main`
2. Make your changes with tests
3. Ensure all CI checks pass locally
4. Open a PR referencing the issue (`Closes #123`)
5. Address review feedback
6. A maintainer will merge once approved

## Testing

- Tests live in `tests/` and use `pytest` with `pytest-asyncio`
- HTTP mocking uses `respx` (not `responses` or `aioresponses`)
- Test both success and error paths
- Test auth modes: api_key, sudo, and oauth where relevant

## What We're Looking For

- Bug fixes with regression tests
- New tools that cover Looker API endpoints not yet exposed
- Documentation improvements
- Performance improvements with benchmarks

## What We're NOT Looking For

- Changes that break the `IdentityProvider` protocol contract
- Adding dependencies without strong justification
- Tool implementations that bypass the `LookerClient.session()` pattern

## License

By contributing, you agree that your contributions will be licensed under the [Apache License 2.0](../LICENSE).
