# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.5.0] - 2026-04-15

### Added

- **connection** tool group (6 tools): database connection CRUD with built-in health checks — enables end-to-end setup of a new Looker instance without leaving MCP.
  - `get_connection`: fetch full configuration for a single connection (dialect, host, PDT settings, etc.)
  - `list_connection_dialects`: discover supported dialects and their accepted options before creating a connection
  - `create_connection`: register a new database connection (all fields except `name` and `dialect_name` are optional and only sent when provided, so Looker defaults are preserved)
  - `update_connection`: partial update — only provided fields are patched; returns an actionable error when no fields are supplied
  - `delete_connection`: remove a connection (warns in the description that dependent LookML will fail)
  - `test_connection`: runs Looker's built-in per-check validator (connect, query, tmp_table, cdt, pdt, kill) and returns a structured breakdown so agents can correct specific failing checks without re-running the full suite
- Total tool count: 98 → 104 across 11 groups

## [0.4.0] - 2026-04-02

### Added

- **RBAC management tools** (20 new tools in admin group):
  - Permission sets: `list_permissions`, `list_permission_sets`, `create_permission_set`, `update_permission_set`, `delete_permission_set`
  - Model sets: `list_model_sets`, `create_model_set`, `update_model_set`, `delete_model_set`
  - Role lifecycle: `get_role`, `update_role`, `delete_role`
  - Group management: `create_group`, `delete_group`
  - Role assignments: `set_role_groups`, `set_role_users`, `set_user_roles`, `get_user_roles`
  - User provisioning: `create_credentials_email`, `send_password_reset`
- Total tool count: 78 → 98 across 10 groups

### Changed

- `LookerSession.put()` body parameter now accepts `list[Any]` in addition to `dict[str, Any]` (needed for Looker's array-body PUT endpoints)

## [0.3.0] - 2026-04-02

### Fixed

- Upgraded fastmcp 2.x → 3.2.0 for CVE-2026-32871 (SSRF in OpenAPI Provider)

## [0.2.0] - 2026-03-19

### Added

- **board** tool group (13 tools): full CRUD for boards, board sections, and board items
  - `list_boards`, `get_board`, `create_board`, `update_board`, `delete_board`
  - `get_board_section`, `create_board_section`, `update_board_section`, `delete_board_section`
  - `get_board_item`, `create_board_item`, `update_board_item`, `delete_board_item`
  - Input validation on `create_board_item` enforcing exactly one of `dashboard_id`, `look_id`, or `url`
- **folder** tool group (9 tools): folder navigation, CRUD, and content listing
  - `list_folders`, `get_folder`, `create_folder`, `update_folder`, `delete_folder`
  - `get_folder_children`, `get_folder_ancestors`
  - `get_folder_looks`, `get_folder_dashboards`
- Total tool count: 56 → 78 across 10 groups

## [0.1.2] - 2026-03-17

### Fixed

- File operations (`list_project_files`, `get_file`, `create_file`, `update_file`,
  `delete_file`) now pass `workspace_id=dev` query parameter, fixing 404 errors on
  dev-mode endpoints.
- Added `params` argument to `LookerSession.post()`, `.patch()`, `.put()`, and
  `.delete()` methods (`.get()` already had it).

### Removed

- `toggle_dev_mode` tool — sessions are ephemeral (per tool call), so `PATCH /session`
  had no lasting effect. File operations now handle workspace context automatically.

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

[0.4.0]: https://github.com/ultrathink-solutions/looker-mcp-server/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/ultrathink-solutions/looker-mcp-server/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/ultrathink-solutions/looker-mcp-server/compare/v0.1.2...v0.2.0
[0.1.2]: https://github.com/ultrathink-solutions/looker-mcp-server/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/ultrathink-solutions/looker-mcp-server/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/ultrathink-solutions/looker-mcp-server/releases/tag/v0.1.0
