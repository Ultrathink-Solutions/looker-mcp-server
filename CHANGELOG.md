# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.13.0] - 2026-04-17

### Added

- **OAuth 2.1 resource-server mode for the MCP endpoint** (MCP 2025-11-25
  authorization). Opt in by setting `LOOKER_MCP_MODE=public`; the server
  then validates every request's `Authorization: Bearer <JWT>` header
  against a configurable authorization server.
  - `LOOKER_MCP_MODE` (`dev` / `public`, default `dev`): posture switch.
    `dev` stays permissive for trust-network and local deployments;
    `public` enables fail-closed startup validation and bearer-token
    enforcement on every request.
  - `LOOKER_MCP_JWKS_URI`, `LOOKER_MCP_ISSUER_URL`,
    `LOOKER_MCP_RESOURCE_URI`: three new required-in-`public`-mode env
    vars binding the JWK Set endpoint (RFC 7517), the expected `iss`
    claim (RFC 8414), and this resource's canonical audience identifier
    (RFC 8707) respectively. All three are validated as non-empty
    absolute `https://` URIs at startup.
- **RS256 / ES256 signature verification** against cached JWKS keys.
  HS256 and other symmetric algorithms are hard-rejected at header
  inspection (RFC 9068 §2.1 / CVE-2015-9235 — algorithm-confusion
  defense). JWKS responses are filtered by both allowlist and JWK type
  (`RS256` requires `kty=RSA`; `ES256` requires `kty=EC`; `kty=oct` is
  unconditionally dropped).
- **JWKS cache** (`looker_mcp_server.oidc.jwks.JWKSCache`) with a 1-hour
  TTL, async-lock-serialized fetches, and a throttled kid-miss refresh
  (≤1 forced refresh per 5 minutes) so a rotation event flows through
  automatically without flooding the authorization server on brute-force
  `kid` values. Transient post-cold-start failures (network, malformed
  JSON, invalid payload shape, zero-usable-keys) preserve the existing
  cache; only cold-start failures raise fail-closed.
- **Protected Resource Metadata** (RFC 9728) served at
  `/.well-known/oauth-protected-resource` and — when
  `LOOKER_MCP_RESOURCE_URI` carries a path — additionally at the
  RFC 9728 §3 suffix-variant location
  `/.well-known/oauth-protected-resource<resource-path>`. The suffix
  variant is the spec-canonical URL and the one referenced by
  `resource_metadata=...` in `WWW-Authenticate` challenges; the
  origin-rooted path is also served as a defensive fallback for
  clients that probe the origin well-known location before following
  the challenge hint. Both paths serve the same document, which
  advertises `resource_signing_alg_values_supported: ["RS256",
  "ES256"]` and `bearer_methods_supported: ["header"]`.
- **Realm-bearing `WWW-Authenticate` challenges** (RFC 7235 §4.1 +
  RFC 9728 §5.1). 401 responses carry `Bearer realm="..."
  resource_metadata="..."`; 403 responses on missing scope emit
  `error="insufficient_scope"` with the required-scope list.
  Quoted-string escaping follows RFC 7230 §3.2.6.
- **Bearer-in-query rejection** per OAuth 2.1 §5.1.1 — `?access_token=`
  and `?authorization=` receive a 400 `invalid_request` on every path
  (including `/healthz` and `/.well-known/*`). URL-bound bearers leak
  into referrer headers, proxy logs, and browser history regardless of
  destination.
- **Typed deployment-posture errors** at startup when `public`-mode
  configuration is incomplete or malformed. All raise
  `DeploymentPostureError` with a `PostureErrorKind` discriminator so
  callers can branch on the structured kind instead of the message:
  `public_missing_jwks_uri`, `public_missing_issuer_url`,
  `public_missing_resource_uri`, `public_resource_uri_not_https`,
  `public_resource_uri_malformed`, `public_static_bearer_forbidden`.

### Changed

- `LOOKER_MCP_JWKS_URI`, `LOOKER_MCP_ISSUER_URL`, and
  `LOOKER_MCP_RESOURCE_URI` are normalized at field-validator stage:
  surrounding whitespace is stripped from all three, and the resource
  URI additionally has a single trailing slash removed. Normalization
  is mode-independent so `dev` deployments also carry canonical values
  downstream.

### Deprecated

- `LOOKER_MCP_AUTH_TOKEN` (static bearer authentication). Emits a
  `DeprecationWarning` at startup when set in `dev` mode. Rejected
  outright in `public` mode (RFC 9068 §2.1 forbids symmetric static
  bearers for OAuth 2.1 access tokens). Scheduled for removal in a
  future major version; migrate to OIDC via the new
  `LOOKER_MCP_MODE=public` configuration.

Total tool count: unchanged (160 tools / 15 groups) — this is an
infrastructure / deployment-posture release, not a tool surface expansion.

## [0.12.0] - 2026-04-15

### Added

- **workflows** tool group — ops + audit compositions (7 new tools added to the group established in 0.11.0). Each orchestrates several Looker API calls with structured partial-failure reporting.
  - `offboard_user`: terminate sessions + revoke API3 credentials + disable (default) or delete user. Non-destructive by default — explicit flag required to delete. `deactivated`/`deleted` flags reflect the actual step outcome, not the request mode.
  - `rotate_api_credentials`: create a new API3 pair (returning the one-time `client_secret`); optional `delete_previous_id` argument handles the retire-after-verify step in the same workflow.
  - `audit_query_activity`: scope enum (`slow`/`errors`/`frequent`/`by_user`/`by_content`) that picks the right `system__activity.history` query shape for common investigations.
  - `audit_instance_health`: composite 3-section health report — failed PDT builds, failed scheduled-plan runs, content validation errors. Reports `sample_count` + `truncated` per section; `healthy` is False when any section errored, was truncated, or has a non-zero issue count.
  - `investigate_runaway_queries`: list running queries above a runtime threshold, optionally `action='kill'` to terminate each.
  - `find_stale_content`: `content_usage` query filtered on `days_since_last_accessed >= N`, sorted oldest-first.
  - `disable_stale_sessions`: enumerate sessions older than N days, optionally `action='terminate'` to force-logout each. Dry-run by default.
- Total tool count: 153 → 160 across 15 groups

## [0.11.0] - 2026-04-15

### Added

- **workflows** tool group (6 tools): Layer 2 task-oriented compositions over the Layer 1 atomic tools. Each orchestrates 2–5 Looker API calls into a single well-sequenced admin job with structured partial-failure reporting. Aligned with Anthropic's tool-design research: fewer higher-level tools improve agent tool-selection accuracy relative to many atomic ones.
  - `provision_connection`: create + test a database connection; returns per-check test breakdown. Connection is left registered even on test failure so the caller can correct the specific failing check.
  - `bootstrap_lookml_project`: create a LookML project, attach it to a git remote, and generate an SSH deploy key. Response includes the public key for installation on the git remote.
  - `deploy_lookml_changes`: write a set of LookML file edits, validate, and — only if validation passes — deploy to production. Only falls back to create on a confirmed 404; other PATCH failures (auth, 5xx) propagate rather than being silently retried.
  - `rollback_to_production`: safe wrapper around `reset_to_production` requiring an explicit `confirm=True` flag, since the operation is destructive.
  - `provision_user`: end-to-end user onboarding in one call — create user + email credentials + role/group assignments + user-attribute values + invite email. Reports per-step status; guards against empty `user_id` from a malformed create response.
  - `grant_access`: idempotent read-modify-write to add a user or group to a role's membership. Preserves existing members.
- Total tool count: 147 → 153 across 15 groups


## [0.10.0] - 2026-04-15

### Added

- **admin** group — schedule and role-membership completion (4 tools):
  - `update_schedule`: PATCH a scheduled plan (was missing; only create/list/delete existed).
  - `run_schedule_once`: trigger a plan outside its cron schedule for manual delivery or smoke testing.
  - `get_role_groups` / `get_role_users`: read current group- and user-membership of a role. Complement existing `set_role_*` setters so callers can read-modify-write safely.
- **modeling** group — datagroup cache management (2 tools):
  - `list_datagroups`: enumerate datagroups with their trigger/stale markers.
  - `reset_datagroup`: invalidate a datagroup's cache by setting `stale_before` to the current unix timestamp.
- **content** group — content-validation audit (1 tool):
  - `validate_content`: run Looker's content validator across all looks and dashboards. Returns broken references grouped by error kind plus totals — useful before users see errors from a LookML change.
- Total tool count: 140 → 147 across 14 groups


## [0.9.0] - 2026-04-15

### Added

- **audit** tool group (13 tools): observability surface covering query history, content usage, PDT/schedule logs, event audit, and live-ops triage.
  - `system__activity` wrappers (5): `get_query_history`, `get_content_usage`, `get_pdt_build_log`, `get_schedule_history`, `get_user_activity_log`. Each composes the right explore + field set + filters over Looker's built-in audit model so callers don't have to know the schema; custom queries can still use the generic `query` tool.
  - Live-ops (8): `list_running_queries` + `kill_query` for active-query triage, `list_active_sessions` + `get_session` + `terminate_session` for session audit and offboarding, and `list_project_ci_runs` + `get_project_ci_run` + `trigger_project_ci_run` for LookML CI visibility.
- Total tool count: 127 → 140 across 14 groups

## [0.8.0] - 2026-04-15

### Added

- **credentials** tool group (12 tools): manage non-email user credentials. Complements the existing `create_credentials_email` in the `admin` group.
  - API3 key-pair lifecycle: `list_credentials_api3`, `create_credentials_api3`, `get_credentials_api3`, `delete_credentials_api3`. `create_credentials_api3` returns the `client_secret` in the response with a prominent one-time-only warning (Looker never surfaces the secret again) — this is the supported rotation path for service-account credentials.
  - LDAP / SAML / OIDC / Google links: `get_credentials_{type}` and `delete_credentials_{type}` for each. Deletion unlinks the user from that identity provider; most providers re-link automatically on the user's next successful sign-in.
- Total tool count: 115 → 127 across 13 groups

## [0.7.0] - 2026-04-15

### Added

- **user_attributes** tool group (11 tools): manage per-user and per-group data entitlements used for row-level security, per-developer git credentials, and LookML filter defaults.
  - Attribute lifecycle: `list_user_attributes`, `get_user_attribute`, `create_user_attribute`, `update_user_attribute`, `delete_user_attribute`
  - Per-group overrides: `list_user_attribute_group_values`, `set_user_attribute_group_values`, `delete_user_attribute_group_value`
  - Per-user overrides: `list_user_attribute_values_for_user`, `set_user_attribute_user_value`, `delete_user_attribute_user_value`
  - `list_user_attribute_values_for_user` surfaces each value's ``source`` (user override / group / default), useful for explaining why a user sees specific LookML behavior.
- Total tool count: 111 → 115 across 12 groups

### Changed

- `LookerSession.post()` and `.patch()` body parameter now accept `list[Any]` in addition to `dict[str, Any]` (needed for `POST /user_attributes/{id}/group_values`, which takes an array body). Matches the `put()` widening from 0.4.0.
- `_path_seg` helper added to `tools/_helpers.py` for consistent URL-encoding of path segments; `connection.py`, `modeling.py`, and `user_attributes.py` now share the single implementation.

## [0.6.0] - 2026-04-15

### Added

- **modeling** group — project lifecycle tools (7 new tools): full LookML project CRUD plus manifest inspection and deploy-key management.
  - `get_project`: fetch full configuration for a single project (git remote, pull-request mode, validation policy, release management flags)
  - `create_project`: provision a new empty project; includes next-step guidance in the response
  - `update_project`: partial update covering git remote settings, pull-request mode, validation, and release management
  - `delete_project`: remove a project
  - `get_project_manifest`: read the parsed LookML manifest (declared dependencies, connection references)
  - `get_project_deploy_key`: read the project's existing SSH deploy public key
  - `create_project_deploy_key`: generate (or rotate) the project's SSH deploy key pair and return the public half for installation on the git remote
- Project-level path parameters are now URL-encoded in all newly added tools so values with reserved characters round-trip correctly.
- Total tool count: 104 → 111 across 11 groups

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

[0.12.0]: https://github.com/ultrathink-solutions/looker-mcp-server/compare/v0.11.0...v0.12.0
[0.11.0]: https://github.com/ultrathink-solutions/looker-mcp-server/compare/v0.10.0...v0.11.0
[0.10.0]: https://github.com/ultrathink-solutions/looker-mcp-server/compare/v0.9.0...v0.10.0
[0.9.0]: https://github.com/ultrathink-solutions/looker-mcp-server/compare/v0.8.0...v0.9.0
[0.8.0]: https://github.com/ultrathink-solutions/looker-mcp-server/compare/v0.7.0...v0.8.0
[0.7.0]: https://github.com/ultrathink-solutions/looker-mcp-server/compare/v0.6.0...v0.7.0
[0.5.0]: https://github.com/ultrathink-solutions/looker-mcp-server/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/ultrathink-solutions/looker-mcp-server/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/ultrathink-solutions/looker-mcp-server/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/ultrathink-solutions/looker-mcp-server/compare/v0.1.2...v0.2.0
[0.1.2]: https://github.com/ultrathink-solutions/looker-mcp-server/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/ultrathink-solutions/looker-mcp-server/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/ultrathink-solutions/looker-mcp-server/releases/tag/v0.1.0
