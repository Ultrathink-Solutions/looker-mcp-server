# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.20.0] - 2026-05-28

Adds a third MCP-level authentication posture, `LOOKER_MCP_MODE=looker_oauth`,
in which **Looker itself is the authorization server** and the MCP server
holds **no admin API3 credentials and no sudo capability**. A client runs a
Looker PKCE flow directly against the Looker instance, obtains an **opaque**
per-user Looker access token, and presents it as `Authorization: Bearer
<token>`. The server advertises Looker (the `LOOKER_BASE_URL`) as the
authorization server in its RFC 9728 Protected Resource Metadata, verifies
every inbound token by calling Looker's `GET /user` introspection endpoint
(accept iff Looker returns a valid user; fail-closed otherwise), and forwards
the verified token to Looker as the session token so the user's own Looker
permissions govern every call. This sidesteps the `X-User-*` identity envelope
entirely and requires `LOOKER_BASE_URL` plus `LOOKER_MCP_RESOURCE_URI` (this
MCP server's own public URI) — no JWKS, issuer, or service-account
configuration.

### Added

- **`LOOKER_MCP_MODE=looker_oauth` — Looker-as-its-own-authorization-server,
  opaque-token, no-credential posture.** Selectable by configuration; requires
  `LOOKER_BASE_URL` and `LOOKER_MCP_RESOURCE_URI` (both absolute `https://`
  URLs — Looker's instance and this MCP server's own public URI). Concretely:
  - **Provider selection** — `create_server` selects a no-credential
    `OAuthIdentityProvider` that reads the bearer straight from the
    `Authorization` header (stripping the `Bearer` scheme) and forwards it to
    Looker as an `oauth`-mode session token. No fallback credentials and no
    `act_as_user` admin-sudo wrapper (this posture has no admin identity to
    impersonate with), so a tokenless request fails rather than borrowing a
    shared identity.
  - **PRM target** — the RFC 9728 Protected Resource Metadata document
    advertises the Looker base URL as `authorization_servers[0]`, so MCP
    clients auto-discover Looker's OAuth endpoints and run PKCE there. The
    `resource` identifier is the required `LOOKER_MCP_RESOURCE_URI` (this MCP
    server's own public URI — it must not point at Looker's host, since the
    MCP server itself serves the PRM).
  - **Opaque-token inbound** — a new ASGI gate
    (`LookerOAuthAuthMiddleware`, in `oidc/looker_introspection.py`) verifies
    each request's opaque bearer via Looker `GET /user` introspection
    (`LookerUserIntrospector`). Accepted iff Looker returns a user; 401
    `invalid_token` on rejected/expired tokens, non-200 responses, non-JSON
    bodies, missing user id, or Looker transport failures (fail-closed). The
    gate mirrors the `public`-mode contract: 400 on URL-query bearers (OAuth
    2.1 §5.1.1), realm-bearing `WWW-Authenticate` challenges on 401, anonymous
    `/.well-known/*` + `/healthz` + `/readyz` + `/_introspect`. On success the
    `Authorization` header is left intact for the downstream identity provider.
  - **Config posture validation** — a new `model_validator` requires
    `LOOKER_BASE_URL` **and** `LOOKER_MCP_RESOURCE_URI` (both `https://`; the
    resource URI is this MCP server's own canonical URI — the RFC 9728 PRM
    `resource` and the host of the `resource_metadata` challenge URL — so it
    cannot default to Looker's host), but **not** `LOOKER_MCP_JWKS_URI` /
    `LOOKER_MCP_ISSUER_URL`. A static `LOOKER_MCP_AUTH_TOKEN` is rejected at
    startup (it would defeat the per-user identity). New `PostureErrorKind`
    values: `looker_oauth_missing_base_url`, `looker_oauth_base_url_not_https`,
    `looker_oauth_base_url_invalid`, `looker_oauth_missing_resource_uri`,
    `looker_oauth_static_bearer_forbidden`.

  Note: the `looker_oauth` route is mounted only on `streamable-http`
  transport, consistent with the `public`-mode auth gate; `stdio` deployments
  are unaffected.

## [0.19.0] - 2026-05-24

Two changes targeting deployments where this server sits behind a
gateway aggregator that fronts multiple MCP servers. First, a new
host-root **`/_introspect`** route serves the MCP discovery slice
(`initialize`, `notifications/initialized`, `tools/list`) without
requiring a user JWT, so the aggregator can populate its tool
catalog at registration time without holding service-account
credentials at this backend — `tools/call` is rejected
(`-32601`); execution still flows through `/mcp` where the user
JWT validates. The route is open by default and operators are
expected to compose at least one of (a) network controls
restricting ingress to `/_introspect` to the trusted aggregator's
pod or subnet, and (b) the optional `LOOKER_MCP_INTROSPECT_BEARER`
env var, which when set requires `Authorization: Bearer <value>`
on all three methods (POST, GET, DELETE) and 401s otherwise. The
endpoint is mounted only on `streamable-http` transport; `stdio`
deployments don't sit behind a gateway and don't get the route.
Second, **`/readyz`** no longer requires `LOOKER_CLIENT_ID` /
`LOOKER_CLIENT_SECRET` in external-identity deployments (OAuth
pass-through, sudo-by-header) where per-request user tokens supply
auth at tool-invoke time. With service-account credentials
configured, behavior is unchanged (live login/logout cycle);
without them, the probe issues a no-auth `HEAD` against
`LOOKER_BASE_URL` and returns 503 only on transport-layer failures
(DNS, connection refused, TLS error, timeout) — fixing a
fail-closed bug where K8s readiness probes would 503 forever and
roll back atomic Helm installs.

### Added

- **Host-root `/_introspect` endpoint for gateway tool discovery
  (open by default; optional shared-bearer guard).** A new route at
  `/_introspect` — mounted alongside but separate from the
  authenticated `/mcp` route — serves the MCP discovery slice
  (`initialize`, `notifications/initialized`, `tools/list`) so a
  gateway aggregator that fronts multiple MCP servers can populate
  its tool catalog at registration time without holding
  service-account credentials at each backend. `tools/call` is
  rejected (`-32601`); execution still flows through `/mcp` where
  the user JWT validates.

  **Security posture — operators must read.** The route is mounted
  at host root (the `/mcp` OAuth 2.1 middleware deliberately does
  not see it). It does NOT require a user JWT, and by default
  accepts requests from any network-reachable caller. Operators
  must compose at least one of the following defenses:

  1. **Network controls (primary; recommended for cluster
     deployments).** Restrict ingress to `/_introspect` so it is
     reachable only from the trusted gateway aggregator's pod or
     subnet. External ingress should expose only `/mcp/*` and the
     well-known PRM document.
  2. **Application-layer shared bearer.** Set
     `LOOKER_MCP_INTROSPECT_BEARER` to any opaque token. When set,
     all three methods (POST, GET, DELETE) require
     `Authorization: Bearer <value>` and respond `401` otherwise;
     the aggregator is configured with the same token out-of-band.
     When unset (the default), the endpoint is **open** and the
     operator is implicitly relying on network-layer isolation.

  The route also accepts `GET` (204 — empty server-push stream
  placeholder) and `DELETE` (200 — session teardown ack) so a
  well-behaved MCP Streamable HTTP client doesn't log 405 errors on
  every discovery cycle. The optional bearer guard, when configured,
  applies uniformly to all three methods.

  Wiring details: the route is mounted only on `streamable-http`
  transport — `stdio` deployments don't sit behind a gateway and
  don't get the route. The OAuth 2.1 resource-server middleware
  (active in `LOOKER_MCP_MODE=public`) exempts `/_introspect` from
  its token check, since the route enforces its own optional bearer
  instead.

  Cited precedent: [Strawberry GraphQL Federation](https://strawberry.rocks/docs/guides/federation),
  [Apollo Federation v1 spec](https://www.apollographql.com/docs/federation/v1/federation-spec/).

### Fixed

- **`/readyz` no longer requires API3 service-account credentials in
  external-identity deployments.** The readiness probe previously
  asserted that both `LOOKER_CLIENT_ID` and `LOOKER_CLIENT_SECRET`
  were set and exercised a live login/logout cycle against the Looker
  API. That contract is correct for service-account mode but wrong
  for deployments where the server has no standing identity — OAuth
  pass-through (`X-User-Token`) or sudo-by-header
  (`X-User-Email`) flows where per-request user tokens supply auth at
  tool-invoke time. Such deployments would deliberately omit the
  service-account credentials, then fail `kubelet` readiness checks
  forever and roll back atomic Helm releases.

  The route now branches on whether both API3 credentials are
  configured. With them, behavior is unchanged (live login/logout
  cycle). Without them, the probe issues a no-auth `HEAD` against
  `LOOKER_BASE_URL` via a new `LookerClient.check_reachability` —
  any HTTP response (including `401`) proves the dependency is
  reachable, only transport-layer failures (DNS, connection refused,
  TLS error, timeout) return `503`. Auth is validated per-request at
  tool-invoke time; readiness is a liveness-of-dependency check.

## [0.18.0] - 2026-05-19

Adds a new opt-in **`render` tool group** wrapping Looker's
`/render_tasks/*` API surface — the async create-poll-fetch pattern
for turning Looker content into PNG, JPG, or PDF. Four tools cover
the full subject matrix: `render_query` (ad-hoc explore that mirrors
the existing `query` tool's `model`/`view`/`fields`/`filters`/`sorts`
surface), `render_look` (saved Look), `render_dashboard` (full
dashboard — the only subject that supports PDF, with
`pdf_paper_size`, `pdf_landscape`, `long_tables`, `theme`, and a
typed `dashboard_filters: dict[str, str]`), and
`render_dashboard_tile` (individual dashboard element). The four
share a `_create_and_wait_for_render_task` helper that owns the
create endpoint POST, the bounded `GET /render_tasks/{id}` poll on
a 0.5s → 1s → 2s capped backoff, and the binary `GET /results`
fetch — so the per-subject paths cannot drift on path shape,
terminal-state handling, or failure-detail propagation. Three
recoverable conditions return JSON escape-hatch payloads carrying
the `render_task_id` so callers can recover an in-flight render
without restarting: a polling-deadline timeout, a `width * height`
over 16-megapixel client-side cap, and an oversized-result hit
against the 10 MB MCP transport budget. A new
`LookerSession.get_bytes(path, *, max_bytes=…)` transport helper
streams the binary response via `httpx.stream()` + `aiter_bytes()`
and short-circuits via `Content-Length` when the server provides
it, so an oversized render never fully materializes in memory —
the streaming early-break is the fallback for servers that omit
the header. The `render` group is added to `ALL_GROUPS` but
**not** to `DEFAULT_GROUPS`: image rendering is opt-in, enabled
with `--groups …,render` or `--groups all`.

### Added

- **`render_query`, `render_look`, `render_dashboard`, and
  `render_dashboard_tile` tools** in a new `render` group
  (`src/looker_mcp_server/tools/render.py`). Binary results
  round-trip through the MCP envelope as FastMCP's typed helpers:
  `fastmcp.utilities.types.Image(format="png"|"jpg")` serializes to
  MCP `ImageContent` (LLM-visible); `File(format="pdf")` serializes
  to `EmbeddedResource` with `BlobResourceContents` (clients render
  or save it). `render_dashboard`'s `dashboard_filters` parameter
  is a typed `dict[str, str]` that's URL-encoded internally into
  the `?Foo=bar&Baz=qux` shape Looker's API expects, keeping the
  surface symmetric with the existing `query` tool's `filters`
  parameter. PDF-only knobs (`pdf_paper_size`, `pdf_landscape`,
  `long_tables`) are gated on `result_format == "pdf"` so an image
  render never carries page-orientation hints Looker has no use
  for; `theme` and `dashboard_filters` stay format-agnostic since
  both apply to all dashboard render formats. `render_query`
  honours the same `dev_mode` + `branch` + `project_id` +
  `act_as_user` parameter set as the existing `query` tool, so the
  ad-hoc explore can be validated against a feature branch with
  atomic save → swap → run → restore semantics. Polling deadlines
  default to 120 s for image renders and 180 s for dashboards
  (PDFs commonly take 30-90 s); on timeout the tool returns
  `{status: "timeout", render_task_id, last_status, runtime_meta}`
  so the caller can fetch the bytes directly from Looker once the
  task finishes.
- **`LookerSession.get_bytes(path, *, max_bytes=None)`** in
  `looker_mcp_server.client`. Returns `(body_bytes, content_type,
  total_bytes, truncated)`. When `max_bytes` is set, the helper
  first inspects `Content-Length` and short-circuits the download
  entirely if the advertised size exceeds the cap; otherwise it
  streams via `aiter_bytes()` and stops appending as soon as the
  running total passes the cap. The transport budget keeps a
  single oversized render from spiking server memory under
  concurrent load. Reuses the same `_raise_for_status` 4xx/5xx
  body-parsing contract as `get` and `get_text`, so structured
  Looker error bodies still reach callers as `LookerApiError`
  carrying the full error envelope.
- **`render` tool group registration** in
  `looker_mcp_server.server`'s `_group_registry` and `ALL_GROUPS`
  (`config.py`). Opt-in only — not added to `DEFAULT_GROUPS` since
  rendering exercises a different transport-budget path than the
  read-oriented default groups. Enable with `--groups …,render`,
  `LOOKER_GROUPS=…,render`, or `--groups all`.

## [0.17.0] - 2026-05-14

Small follow-up release adding **`run_query`**, the missing peer to
`run_look`: it wraps `GET /queries/{query_id}/run/{result_format}` so
callers can re-run an existing saved `Query` by ID without re-specifying
its body. The previously-available `query` tool always re-specifies the
query from `model/view/fields/filters/sorts`, which silently drops
anything not in that body — `dynamic_fields`, table calcs, vis config,
and other settings baked into the saved `Query` object. The motivating
use case is **tile-fidelity validation**: a dashboard tile's
`query.id` points to a saved `Query` that may carry tile-local table
calcs in its `dynamic_fields`, and faithfully reproducing the tile's
data requires running *that* `Query`, not re-specifying a new one.
`run_query` also exposes the Looker run-time knobs that matter for
that workflow — `apply_formatting` (render values per LookML/Look
formatting), `apply_vis` (apply vis-config rendering),
`server_table_calcs` (compute table calcs server-side), `cache`, and
a `limit` override. `run_dashboard`'s per-element call now routes
through the same shared helper that backs `run_query`, so the two
paths can't drift on path shape, query-string serialization, or the
JSON-vs-`text/plain` response routing.

### Added

- **`run_query(query_id, ...)` tool** in the `query` group. Wraps
  `GET /api/4.0/queries/{query_id}/run/{result_format}`. Accepts
  `result_format` (`json`/`json_detail`/`csv`/`txt`), `limit`,
  `apply_formatting`, `apply_vis`, `server_table_calcs`, `cache`,
  plus the standard `dev_mode` + `branch` + `project_id` +
  `act_as_user` parameter set for parity with `run_look`. `csv` and
  `txt` formats route through `session.get_text` (the same
  `text/plain` trap that motivated v0.16.0's `query_sql` fix) and
  are wrapped in a `{"format": ..., "data": ...}` JSON envelope so
  the MCP response shape stays JSON.
- **Shared `_execute_saved_query` helper** at module level in
  `tools/query.py`. Both `run_query` and `run_dashboard`'s per-tile
  loop call it, so the saved-query execution path (URL shape, param
  serialization, response routing) has a single source of truth.
  Booleans are serialized as lowercase `true`/`false` — httpx's
  default `str(True)` → `"True"` is not what Looker's query-string
  parser accepts; pinned by tests.

### Changed

- `run_dashboard` no longer inlines its per-element
  `session.get(f"/queries/{id}/run/json")` call; it now routes
  through `_execute_saved_query` for consistency with `run_query`.
  Behavior is unchanged — same endpoint, same response shape — but
  the wire-level contract is now regression-guarded by a test.

## [0.16.0] - 2026-05-10

This release closes the **dev-mode gap** across every workspace-scoped
tool group, adds the load-bearing primitive for catching LookML
data-regression bugs in CI (`run_lookml_tests`), and introduces full
datagroup + PDT build administration. Query, modeling, and the
dev-mode-required git tools now accept a uniform `dev_mode` + `branch`
+ `project_id` + `act_as_user` triad — set `branch=…` to atomically
swap the dev workspace to a feature branch for the call, run the
operation, and restore the saved branch in a `finally` block (even
when the body raises). `validate_project` previously had no dev-mode
support at all and silently validated production LookML even when
called against a feature branch; that's now an explicit opt-in via
the same triad. The new `LookerSession.update_workspace` and
`LookerSession.use_branch` primitives in `client.py` are reusable by
custom tools that want the same atomic semantics. A new `identity`
tool group exposes `whoami` so callers can confirm the active session
identity (especially useful when a Looker instance has multiple
similarly-named users and per-call `act_as_user` impersonation is in
play). The `LookerApiError.body` field now preserves the full Looker
error envelope on 4xx/5xx responses, surfacing high-signal debugging
fields like `sql` (compiled SQL on query failures), `errors[]`
(LookML compile/evaluator diagnostics), and `applied_filters`.

### Added

- **Universal dev-mode parameters across query, modeling, and git
  tool groups.** Query tools (`query`, `query_sql`, `query_url`,
  `run_look`) and modeling tools (`list_project_files`, `get_file`,
  `create_file`, `update_file`, `delete_file`, `validate_project`,
  `list_lookml_tests`, `run_lookml_tests`) accept a uniform `dev_mode`,
  `branch`, `project_id`, `act_as_user` parameter set. Setting
  `branch=…` implies `dev_mode=True` and triggers atomic save → swap
  → run → restore semantics: the dev workspace's currently-checked-out
  branch is captured before the call and restored on exit (success or
  failure). The four dev-mode-required git tools (`switch_git_branch`,
  `create_git_branch`, `delete_git_branch`, `reset_to_production`)
  now default `dev_mode=True` so they no longer fail with
  `400 Developer mode required`. `act_as_user` is propagated through
  the same `ArgumentSudoIdentityProvider` machinery as in v0.15.0,
  enabling the canonical CI pattern: sudo as a dedicated `ci-bot`
  user, swap to the feature branch, validate or query, restore. See
  the new *Dev Mode and Branch Validation* section in the README for
  the four canonical workflows (one-shot CI, prod vs PR diff,
  iterative human debug, cleanup another user's stuck workspace) and
  the per-Looker-user concurrency caveat.
- **`LookerSession.update_workspace(workspace_id)` and
  `LookerSession.use_branch(project_id, branch_name)`** in
  `looker_mcp_server.client`. The first wraps Looker's
  `PATCH /session` with `{"workspace_id": "dev" | "production"}`;
  the second is an `@asynccontextmanager` that performs the atomic
  save/swap/restore cycle. Reusable by custom tools that need the
  same semantics. `use_branch` fails fast with `LookerApiError` if
  Looker returns a malformed payload without a `name` field — the
  guard prevents a downstream `PUT {"name": null}` that would leave
  the workspace stuck on the caller-supplied branch.
- **`list_lookml_tests` and `run_lookml_tests`** in the modeling
  group. These are the load-bearing primitives for catching
  data-regression bugs introduced by a PR. Looker compiles each
  test's `explore_source` query, runs it against the warehouse, and
  evaluates the assertion expression against the result rows — pair
  with `branch=…` to validate a feature branch before merge. The
  `run_lookml_tests` per-call timeout defaults to 1800 seconds (30
  minutes, matching what Spectacles uses for the same endpoint)
  because data tests run real warehouse queries with assertions on
  potentially large tables; non-positive values are rejected with a
  clear `ValueError` before any Looker call. Failures pass through
  the raw assertion-level detail (`model_name`, `test_name`,
  `success`, `errors[]`) since that's exactly what a regression
  report needs.
- **`identity` tool group with `whoami`.** New default-enabled tool
  group exposing a single `whoami` tool that calls `GET /user` and
  returns a stable allow-listed subset (`id`, `display_name`,
  `email`, `first_name`, `last_name`, `role_ids`, `group_ids`,
  `verified_looker_employee`, `is_disabled`). When the session is
  sudo-impersonating another user (per-call `act_as_user` argument
  or `X-User-Token` header), `whoami` returns the impersonated user's
  record because Looker resolves `GET /user` against the active
  bearer token. The field allow-list is deliberate: Looker adds new
  fields to its user response over time, and a permissive default
  would surface them without a maintainer deciding they're
  appropriate.
- **Datagroup administration**: `get_datagroup` (single-datagroup
  detail with allow-listed fields) and `trigger_datagroup` (sets
  `triggered_at` to force PDT rebuild *and* cache invalidation
  simultaneously). `trigger_datagroup` is the missing primitive that
  distinguishes from `reset_datagroup` — the latter only updates
  `stale_before` (cache bust without a rebuild).
- **PDT build administration**: `start_pdt_build`, `check_pdt_build`,
  `stop_pdt_build`, `graph_derived_tables_for_view`,
  `graph_derived_tables_for_model`. Per Looker's OpenAPI 4.0 spec,
  both `start_pdt_build` and `stop_pdt_build` are GET (not POST and
  DELETE) — the OSS server matches that surprising shape with
  regression-locking tests. `start_pdt_build` accepts `force_rebuild`
  and `force_full_incremental` flags plus an optional `workspace`
  selector for dev/production materialization. `check_pdt_build`
  returns status + progress ratio + resource usage. The graph tools
  return DOT-language dependency descriptions, with optional color
  coding for build state on `graph_derived_tables_for_model`. The
  README documents canonical *disable PDT workflow* and *enable PDT
  workflow* recipes that compose these primitives with
  `update_connection`'s `pdt_api_control_enabled` toggle —
  intentionally exposed as separate primitive calls rather than a
  single composite tool so each call emits its own audit line under
  `act_as_user`.
- **`LookerSession.get` accepts a per-call `timeout` override.**
  When set, it replaces the connection-level default for that one
  request via `httpx.Timeout(timeout)`. Used by `run_lookml_tests`
  and reusable by custom tools that hit other long-running endpoints.
- **`ActAsUser` annotation moved from `tools/git.py` to
  `tools/_helpers.py`** so query, modeling, and git tools can share
  it without coupling. `_validate_branch_args` and `_maybe_use_branch`
  helpers also live in `_helpers.py` so any tool that accepts a
  `branch=…` argument has a one-line adoption path.

### Fixed

- **Full Looker error body preserved on 4xx/5xx responses.**
  `LookerApiError` now carries an optional `body: dict | None`
  populated when the response decodes as a JSON object;
  `format_api_error` surfaces it under `body:` in the result. Looker
  query failures (e.g. an evaluator error from a malformed
  `tests.lkml` assert) include the fully compiled SQL and the
  LookML errors[] array in the body — previously stripped by the
  formatter, forcing operators to fall back to direct REST or
  browser DevTools to recover them. Plain-text bodies, JSON arrays,
  and unparseable bodies all leave `body=None` (the contract is
  "dict or nothing"). Non-string `message`/`error` fields are
  defensively coerced into a string for `detail` so downstream
  string ops never see surprises.
- **`query_sql` no longer fails on every Looker connection.** The
  endpoint at `GET /queries/{id}/run/sql` returns `text/plain` (the
  compiled SQL as a raw string), but the previous code routed
  through `session.get` (which calls `response.json()`), so the JSON
  decoder raised before the SQL string could be returned. The fix
  swaps to `session.get_text`, mirroring the pattern already used by
  the git deploy-key tools.
- **`validate_project` previously validated production LookML
  unconditionally**, silently misleading any CI workflow that called
  it against a feature branch and expected per-PR diagnostics. The
  new `dev_mode` and `branch` parameters opt the call into dev-
  workspace validation; default behavior validates production
  (preserving backwards-compatibility for existing callers).
- **Modeling file ops** (`list_project_files`, `get_file`,
  `create_file`, `update_file`, `delete_file`) migrated from the
  undocumented `?workspace_id=dev` query-param trick to the
  canonical session-level `PATCH /session` workspace switch. Read
  tools default `dev_mode=True` (matching previous behavior); write
  tools always operate on dev (Looker rejects writes to production).
  All five accept `branch=…` for atomic save/swap/restore and
  `act_as_user` for the CI service-user pattern.
- **`stop_pdt_build` no longer hard-codes `stopped: True`.** The
  field is now derived from `status == "stopped"` so a no-op stop
  call (e.g., the materialization had already completed naturally)
  correctly reports `stopped: False` instead of falsely claiming
  successful cancellation.
- **`_validate_branch_args` rejects empty/whitespace branch
  strings.** Without the guard, an empty `branch` would have been
  forwarded to `LookerSession.use_branch` and reached Looker as
  `{"name": ""}` — surfacing as an opaque 400 instead of a clean
  validation error.

## [0.15.0] - 2026-05-07

This release adds **per-call admin impersonation** to the git tool
group. The eight workspace-scoped git tools accept an optional
`act_as_user` argument (numeric Looker user ID or email) that scopes
the call to a sudo session targeting that user — closing the
per-user dev-workspace cleanup gap that previously forced operators
to leave the MCP for raw HTTP. Argument-driven sudo is gated by
`LOOKER_SUDO_AS_USER` and refuses with a clear validation error
when sudo is disabled, so the kill switch remains the single control
point for sudo-capable behavior. Argument-driven impersonation emits
a distinct INFO-level audit event (`looker.audit.act_as_user`) so
downstream pipelines can scope on admin per-call sudo independently
of header-driven gateway sudo.

### Added

- **Per-call admin impersonation via `act_as_user`** on every git
  workspace-scoped tool: `get_git_branch`, `list_git_branches`,
  `get_git_branch_by_name`, `create_git_branch`, `switch_git_branch`,
  `delete_git_branch`, `deploy_to_production`, `reset_to_production`.
  Looker dev mode is per-user-isolated, so admin cleanup of another
  user's stuck dev-workspace state previously required leaving the MCP
  for raw HTTP. The new argument accepts either a numeric Looker user ID
  or an email address (resolved via Looker's user-search API) and
  scopes the call to a sudo session targeting that user. Gated by
  `LOOKER_SUDO_AS_USER` — that flag is the single kill switch for
  sudo-capable behavior, and `act_as_user` respects it: passing the
  argument with sudo disabled raises a validation error rather than
  silently running the call under the configured identity. The
  argument is validated up front (must be all-digits or contain `@`)
  so malformed input fails before reaching Looker. Capability is
  enforced server-side by Looker — `login_user` returns 403 if the
  configured admin credentials cannot impersonate, and the MCP
  surfaces the failure as a clean tool error. Email-lookup misses
  raise rather than silently falling back to the configured identity,
  so a typo'd email cannot run an action under the wrong user. See
  *Per-Call Admin Impersonation* in the README for the full security
  model and audit-log shape.
- **`ArgumentSudoIdentityProvider`** in `looker_mcp_server.identity` —
  wraps any inner `IdentityProvider` and reads `act_as_user` from
  `RequestContext.arguments`. Wired automatically when the server is
  constructed with default credentials; caller-supplied identity
  providers are left untouched.
- **`triggered_by` field on `LookerIdentity`** — discriminates
  argument-driven sudo (`"argument"`) from gateway/header-driven sudo
  (`"header"`) so audit consumers can tell admin per-call impersonation
  from automatic gateway routing.
- **`looker.audit.act_as_user` structlog event** — emitted at INFO
  level on every argument-driven sudo with `tool`, `target_user_id`,
  `target_user_email`, `triggered_by`, and `configured_user`. Distinct
  from the existing trace-level `looker.session.sudo` debug line so
  downstream pipelines can scope on audit-only events.

## [0.14.0] - 2026-04-28

This release adds end-to-end agentic management of Looker installations:
every writable field on `DBConnection`, `WriteScheduledPlan`, and the user
schemas is now reachable; the git, scheduling, and credential lifecycles
are fully covered with deploy-key rotation, connection diagnostics,
delegated ownership, conditional delivery, and TOTP enrollment. Every
tool that builds a request body validates preflight (multiple-target
guards, mutual-exclusion guards, required-field guards) so misconfigured
calls return actionable errors instead of opaque Looker 422s.

### Added

- **Git deploy-key management** for LookML projects:
  - `get_git_deploy_key` — fetch the public SSH deploy key Looker uses
    to authenticate to the project's git remote.
  - `create_git_deploy_key` — generate or rotate the deploy key.
    Returns the new public key for registration on GitHub / GitLab /
    Bitbucket. Closes the gap on credential-rotation workflows that
    previously required manual UI clicks per tenant.
- **Git connection diagnostics**:
  - `list_git_connection_tests` — enumerate available diagnostic
    tests for a project's git remote.
  - `run_git_connection_test` — run a single test, returning
    pass/fail status with a human-readable failure cause. Supports
    the `remote_url` and `use_production` query params for testing
    remote dependencies and production credentials.
- **Branch management**:
  - `get_git_branch_by_name` — get a specific branch's full state
    (ref, remote, ahead/behind, error) by name.
  - `delete_git_branch` — delete a local branch (sweeps abandoned
    dev branches that accumulated during iterative LookML work).
- **Full writable surface on `create_user` / `update_user`.** New
  parameters: `home_folder_id`, `locale`, `ui_state`,
  `models_dir_validated`, `can_manage_api3_creds`. `create_user` also
  gains an explicit `is_disabled` parameter for staged rollouts.
- **`update_group` tool** — previously the group surface had create
  and delete but no update, leaving group rename / `can_add_to_content_metadata`
  toggling unreachable.
- `create_group` now accepts `can_add_to_content_metadata`.
- **Group hierarchy management**:
  - `add_group_to_group` — make one group a sub-group of another so
    parent role bindings propagate.
  - `remove_group_from_group` — inverse.
  - `list_group_groups` — enumerate sub-groups under a parent.
  - `list_group_users` — enumerate direct user members of a group
    (visibility companion to `add_group_user` / `remove_group_user`).
- **Full `WriteScheduledPlan` field surface on `create_schedule` and
  `update_schedule`.** Every writable field of Looker's
  `WriteScheduledPlan` schema is now reachable, so all delivery
  configurations are setable from an MCP client:
  - **Targets**: `lookml_dashboard_id` and `query_id` join the
    existing `look_id` / `dashboard_id` (exactly one is required).
  - **Destinations**: a new `destinations` parameter accepts the full
    `ScheduledPlanDestination` array — supports `email`, `webhook`,
    `s3`, and `sftp` types, with `format`, `apply_formatting`,
    `apply_vis`, `parameters` (JSON string), `secret_parameters`
    (write-only JSON for credentials), and `message`. The pre-existing
    `recipients` shorthand still builds an email-only destinations
    array; the two are mutually exclusive.
  - **Conditional delivery**: `require_results`, `require_no_results`,
    `require_change`, `send_all_results`.
  - **Trigger options**: `enabled`, `run_once`, `datagroup`,
    `timezone`, plus `user_id` for delegated ownership.
  - **PDF/render options**: `pdf_paper_size`, `pdf_landscape`,
    `long_tables`, `inline_table_width`, `color_theme`, `embed`.
  - **Branded URLs**: `show_custom_url`, `custom_url_base`,
    `custom_url_params`, `custom_url_label`.
  - **Filters**: `filters_string`.
- **Email-credential lifecycle** (admin group):
  - `get_credentials_email` — read email-credential metadata
    (timestamps, password-reset URL state, `has_password`).
  - `update_credentials_email` — PATCH the credentials object to
    rename the user's login email or set
    `forced_password_reset_at_next_login`. Email is the canonical
    rename path because the User schema has no settable `email`
    field.
  - `delete_credentials_email` — remove the email/password
    credential link entirely.
  - `create_credentials_email` now accepts
    `forced_password_reset_at_next_login` for bootstrapping users
    with temporary passwords issued out-of-band.
- **TOTP (two-factor) lifecycle** (credentials group):
  - `get_credentials_totp` — read TOTP enrollment state
    (`verified`, `is_disabled`, `created_at`).
  - `create_credentials_totp` — enroll a user in TOTP. The user
    completes verification with their authenticator app on next
    sign-in.
  - `delete_credentials_totp` — clear TOTP enrollment so a user
    can re-enroll with a new device.
- **API3 metadata update** (credentials group):
  - `update_credentials_api3` — PATCH an API3 credential pair to
    set its `purpose` field (free-form description used to identify
    what an API key is for during audits).

- **Full `DBConnection` field surface on `create_connection` and
  `update_connection`.** The connection-management tools now expose every
  writable field of Looker's `DBConnection` schema, so connections can be
  configured end-to-end from an MCP client without falling back to the
  Looker admin UI. New fields include:
  - **Key-pair authentication** (Snowflake, BigQuery service-account
    keys): `uses_key_pair_auth`, `certificate` (write-only, base64), and
    `file_type` (`.json` / `.p8` / `.p12`).
  - **OAuth / Application Default Credentials**:
    `oauth_application_id`, `uses_application_default_credentials`,
    `impersonated_service_account`.
  - **Per-user / user-attribute scoping**: `user_db_credentials` and
    `user_attribute_fields` — the explicit allowlist of connection
    fields that draw their values from Looker user attributes at query
    time.
  - **SSH tunneling**: `tunnel_id`, `custom_local_port`.
  - **Oracle TNS**: `uses_tns`, `service_name`.
  - **PDT controls**: `tmp_db_host`, `pdt_concurrency`,
    `pdt_api_control_enabled`, `always_retry_failed_builds`,
    `maintenance_cron`, and `pdt_context_override` (the
    `DBConnectionOverride` block, accepted as a pass-through dict so
    PDT builds can run against a separate write-capable role).
  - **SQL governance**: `max_queries`, `max_queries_per_user`,
    `max_billing_gigabytes` (BigQuery), `cost_estimate_enabled`,
    `query_holding_disabled`, `disable_context_comment`,
    `query_timezone`, `db_timezone`, `after_connect_statements`,
    `connection_pooling`, `sql_runner_precache_tables`,
    `sql_writing_with_info_schema`.
  - **JDBC**: `named_driver_version_requested`.
  - **BigQuery**: `bq_storage_project_id`, `bq_roles_verified`.

- **MCP-tool-schema regression tests.** The connection test suite now
  asserts the registered tool's input schema against the canonical set
  of writable `DBConnection` fields, catching silent drops on future
  refactors.

- **Field-clearing escape hatch on `update_connection`.** New
  `clear_fields: list[str]` parameter explicitly nulls a previously-set
  field on the wire (`oauth_application_id`, `service_name`,
  `tunnel_id`, `after_connect_statements`, `user_attribute_fields`,
  `pdt_context_override`, `impersonated_service_account`, etc.). The
  prior `_set_if`-based body builder dropped explicit `None`, so
  callers had no way to revert a field to its dialect default.
  Validates entries against the canonical writable-field set and
  refuses the `set + clear same field` contradiction.
  `WRITABLE_DBCONNECTION_FIELDS` is exported as a single source of
  truth shared by the runtime validator and the regression tests.

### Changed

- `deploy_to_production` now accepts optional `branch` and `ref`
  query params to deploy a specific named branch or commit, matching
  the spec for `POST /projects/{id}/deploy_ref_to_production`.
  Omitting both preserves the previous default of deploying the
  current dev ref.
- **All git tools now URL-encode `project_id` and `branch_name`**
  via the shared `_path_seg` helper, finishing the rollout
  (previously only the new tools did). Branch names containing `/`
  (e.g. `feature/foo`) and project ids with reserved characters now
  route to the correct endpoint regardless of which git tool is
  invoked.
- **Deploy-key endpoints now use the new
  `LookerSession.get_text` / `post_text` helpers**. Looker's
  `/projects/{id}/git/deploy_key` returns a raw SSH public key as
  `text/plain`, not JSON; the previous `session.get` / `session.post`
  call path would have raised on `response.json()` in production.
  Tests mock the response with `text=` and the correct content-type.
- `create_user`'s `email` parameter is now optional. Previously it
  was required, which forced callers to invent an email even for
  SSO-only setups; with this change SSO-only flows can create users
  without email and let SSO link credentials on first login.
- `update_user` returns an actionable error when no fields are
  provided (matching the pattern already used by `update_schedule`),
  rather than issuing an empty PATCH.
- `create_schedule` now validates that **exactly one** of `look_id`,
  `dashboard_id`, `lookml_dashboard_id`, or `query_id` is provided —
  returns an actionable error otherwise (previously, omitting all four
  was silently accepted and rejected later by Looker).
- `update_schedule` and `create_schedule`'s `destinations` and
  `recipients` parameters use `is not None` semantics rather than
  truthy checks, so an explicit empty list is detected as
  "argument supplied" rather than silently dropped.
- Both `create_schedule` and `update_schedule` now reject the
  `crontab` + `datagroup` combination up front (the two are mutually
  exclusive trigger modes per the WriteScheduledPlan spec). Previously
  the request would have been forwarded to Looker, which returns a
  less actionable error.
- `get_credentials_email` and `get_credentials_totp` now return a
  curated metadata subset matching their docstring contracts rather
  than forwarding the raw upstream payload. This pins the MCP
  response shape across Looker versions and prevents accidental
  exposure of sensitive fields — most importantly the one-time
  `password_reset_url` and `account_setup_url` tokens that Looker
  may include in `GET /credentials_email` responses but that should
  never round-trip through the tool surface.
- `get_connection`'s description now enumerates the read-only metadata
  it surfaces (`pdts_enabled`, `uses_oauth`, `managed`, `last_regen_at`,
  …) so callers know they can read these fields even though they are
  not settable.

### Removed

- `update_user` no longer accepts an undocumented `email` parameter.
  Email address is not a writable field on the User schema in Looker
  4.0 — it is set by replacing the user's `credentials_email` object
  via the credentials tool group.
- `pdts_enabled` and `uses_oauth` are no longer accepted as
  `create_connection` / `update_connection` parameters. Both fields are
  marked `readOnly` on the Looker 4.0 spec — sending them was silently
  ignored by the API. PDTs are enabled implicitly by setting
  `tmp_db_name` and granting the appropriate database permissions;
  OAuth is enabled by setting `oauth_application_id`.

### Internal

- New `LookerSession.get_text` and `post_text` methods for endpoints
  that return `text/plain` (currently the deploy-key endpoints; future
  text-returning endpoints can reuse this).


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

[Unreleased]: https://github.com/ultrathink-solutions/looker-mcp-server/compare/v0.20.0...HEAD
[0.20.0]: https://github.com/ultrathink-solutions/looker-mcp-server/compare/v0.19.0...v0.20.0
[0.19.0]: https://github.com/ultrathink-solutions/looker-mcp-server/compare/v0.18.0...v0.19.0
[0.18.0]: https://github.com/ultrathink-solutions/looker-mcp-server/compare/v0.17.0...v0.18.0
[0.17.0]: https://github.com/ultrathink-solutions/looker-mcp-server/compare/v0.16.0...v0.17.0
[0.16.0]: https://github.com/ultrathink-solutions/looker-mcp-server/compare/v0.15.0...v0.16.0
[0.15.0]: https://github.com/ultrathink-solutions/looker-mcp-server/compare/v0.14.0...v0.15.0
[0.14.0]: https://github.com/ultrathink-solutions/looker-mcp-server/compare/v0.13.0...v0.14.0
[0.13.0]: https://github.com/ultrathink-solutions/looker-mcp-server/compare/v0.12.0...v0.13.0
[0.12.0]: https://github.com/ultrathink-solutions/looker-mcp-server/compare/v0.11.0...v0.12.0
[0.11.0]: https://github.com/ultrathink-solutions/looker-mcp-server/compare/v0.10.0...v0.11.0
[0.10.0]: https://github.com/ultrathink-solutions/looker-mcp-server/compare/v0.9.0...v0.10.0
[0.9.0]: https://github.com/ultrathink-solutions/looker-mcp-server/compare/v0.8.0...v0.9.0
[0.8.0]: https://github.com/ultrathink-solutions/looker-mcp-server/compare/v0.7.0...v0.8.0
[0.7.0]: https://github.com/ultrathink-solutions/looker-mcp-server/compare/v0.6.0...v0.7.0
[0.6.0]: https://github.com/ultrathink-solutions/looker-mcp-server/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/ultrathink-solutions/looker-mcp-server/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/ultrathink-solutions/looker-mcp-server/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/ultrathink-solutions/looker-mcp-server/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/ultrathink-solutions/looker-mcp-server/compare/v0.1.2...v0.2.0
[0.1.2]: https://github.com/ultrathink-solutions/looker-mcp-server/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/ultrathink-solutions/looker-mcp-server/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/ultrathink-solutions/looker-mcp-server/releases/tag/v0.1.0
