# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.1] - 2026-03-01

### Fixed

- Handle Kubernetes service-discovery env var collisions: when deployed as a
  K8s Service named "looker", auto-injected `LOOKER_PORT=tcp://...` and
  `LOOKER_HOST=tcp://...` values no longer crash config parsing.

## [0.1.0] - 2026-03-01

### Added

- Initial release with 56 tools across 8 groups
- **explore** group: `list_models`, `get_model`, `get_explore`, `list_dimensions`, `list_measures`, `list_connections`
- **query** group: `query`, `query_sql`, `run_look`, `run_dashboard`, `query_url`, `search_content`
- **schema** group: `list_databases`, `list_schemas`, `list_tables`, `list_columns`
- **content** group: `list_looks`, `create_look`, `update_look`, `delete_look`, `list_dashboards`, `create_dashboard`, `update_dashboard`, `delete_dashboard`, `add_dashboard_element`, `add_dashboard_filter`, `generate_embed_url`
- **health** group: `health_pulse`, `health_analyze`, `health_vacuum`
- **modeling** group: `list_projects`, `list_project_files`, `get_file`, `create_file`, `update_file`, `delete_file`, `toggle_dev_mode`, `validate_project`
- **git** group: `get_git_branch`, `list_git_branches`, `create_git_branch`, `switch_git_branch`, `deploy_to_production`, `reset_to_production`
- **admin** group: `list_users`, `get_user`, `create_user`, `update_user`, `delete_user`, `list_roles`, `create_role`, `list_groups`, `add_group_user`, `remove_group_user`, `list_schedules`, `create_schedule`, `delete_schedule`
- Three authentication modes: API key, admin sudo, OAuth pass-through
- `DualModeIdentityProvider` for automatic sudo/OAuth routing based on deployment type
- Pluggable `IdentityProvider` protocol for custom authentication
- Dual transport: stdio and streamable-http
- Health endpoints: `/healthz` (liveness) and `/readyz` (readiness with connectivity check)
- Selective tool loading via `--groups` CLI flag
- MCP-level bearer token authentication
- ASGI header capture middleware for per-request identity

[Unreleased]: https://github.com/ultrathink-solutions/looker-mcp-server/compare/v0.1.1...HEAD
[0.1.1]: https://github.com/ultrathink-solutions/looker-mcp-server/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/ultrathink-solutions/looker-mcp-server/releases/tag/v0.1.0
