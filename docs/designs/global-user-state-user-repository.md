# Global user state user repository extraction

## Context

`sky/global_user_state.py` is the stable persistence facade for users,
clusters, credentials, and several legacy state repositories.  Schema,
operator-notification, cloud-check, system-configuration, cluster-YAML,
storage, volume, and service-account-token implementations already live
behind that facade, but the file remains 3,544 lines.

Line count is only a prioritization signal.  This extraction is justified by
a complete repository seam: eight user-row operations share one table,
projection, and caller family while owning none of the cluster lifecycle,
history, credentials, or external authorization policy around them.

## Before: responsibility map

| Responsibility | Callers | Dependencies | State owned | Failure modes | Performance sensitivity | Change cadence |
| --- | --- | --- | --- | --- | --- | --- |
| Shared database lifecycle and compatibility facade | Server startup, all global-state repositories, migrations, tests | `DatabaseManager`, SQLAlchemy metadata and migrations | Engine, pool, session construction, table identities, migration ordering | Wrong database, migration drift, broken import or monkeypatch seams | Cold initialization and connection counts | Database topology and schema evolution |
| User-row persistence and projection | Auth registration, user and service-account APIs, RBAC, jobs, workspaces, dashboard and CLI projections | `models.User`, `user_table`, SQLite/PostgreSQL inserts, SQLAlchemy sessions, wall clock | User identity, display name, password hash, creation time, user type, preferred workspace | Duplicate-name races, dialect drift, partial projection, missing user, unsupported dialect, extra connection checkout | Auth-path latency, one engine lookup per standalone call, passed-session reads must not acquire another connection | Authentication, user administration, workspace preference and dashboard presentation |
| Cluster lifecycle, history, events, and usage persistence | Core cluster operations, refresh, managed jobs, Serve, dashboard | Serialized handles, resource and status models, YAML, event and usage tables | Cluster identity and lifecycle projections | Stale writes, invalid lifecycle transitions, serialization drift, transaction races | Hot refresh paths, batching, query and lock counts | Cluster lifecycle and observability changes |
| Credential and SSH-key persistence | Credential setup and SSH consumers | Pickle, encryption and key material | Cloud credentials and SSH key pairs | Serialization drift, corrupt material, unsafe replacement | Login and launch latency, copy size | Credential and security changes |
| Extracted storage, volume, configuration, token, notification, and cloud-check repositories | Domain-specific APIs and workers | Independent tables, codecs and lifecycle models | Their respective durable rows | Domain-specific corruption and transaction failures | Domain-specific query and import costs | Independent product-domain changes |

The user-row family has materially different callers, dependencies, durable
state, failure behavior, and reasons to change from the surrounding cluster
or credential repositories.  Each operation can own its complete query or
transaction boundary.  The one nested read, `get_user(..., session=...)`, can
retain the caller-owned transaction without importing the facade.

## Decision

Add `sky/global_user_state_users.py` as a plain repository module.  Keep the
historical functions in `sky.global_user_state` as the public facade.

The facade retains:

- public import paths, signatures, module and qualified-name identities, and
  timing and retry decorator depth;
- late-bound `_db_manager`, `orm.Session`, `user_table`, SQLite/PostgreSQL
  dialect helpers, wall clock, and SQLite `RETURNING` capability seams;
- authorization policy in `sky/users` and `sky/workspaces`, outside the raw
  repository;
- direct internal calls through the historical facade, including nested
  cluster projections that depend on monkeypatching `get_user`.

The implementation receives those dependencies explicitly and owns user-row
SQL, projection, and commit ordering.  The facade passes its existing shared
`_session_scope(session)` context manager to `get_user`; the repository queries
inside that scope and projects the row only after the scope exits.  This
preserves the established caller-session, lifecycle-order, and
connection-budget seams without duplicating session-scope logic.

This is facade-first decomposition using functions and a module.  A class,
protocol, abstract repository, registry, strategy, or dependency-injection
container would invent a second implementation that does not exist.  Moving
the public functions directly would break established import, monkeypatch,
and decorator identities.  Moving or duplicating `_session_scope` would
expand the change beyond user-row persistence and create two owners for the
process connection-budget invariant.

## Behavior contract

- User table columns, constraints, durable values, and `models.User` identity
  do not change.
- `add_or_update_user` keeps its `name is None` short circuit, created-time
  rule, duplicate-name check, password and type update rules, SQLite
  `RETURNING` fallback, PostgreSQL `xmax` sentinel, return shapes, transaction
  ordering, and unsupported-dialect error.
- Point, batch, exact-name, partial-name, and all-user reads preserve their
  current projections.  In particular, partial-name results continue to omit
  the password value while the other reads include it.
- `get_user(..., session=...)` reuses the supplied session and performs no
  engine lookup.  Without a session it performs exactly one lookup and owns
  exactly one session.  Row projection occurs after the shared session scope
  exits, matching the historical lifecycle order.
- Deletion and preferred-workspace writes preserve commit and row-count
  behavior.  Workspace access validation remains a caller responsibility.
- Every public symbol remains defined by `sky.global_user_state`; no schema,
  migration, CLI, wire, configuration, serialization, authorization, or
  lifecycle contract changes.

## Alternatives

- Leave the functions in the facade: safe, but retains a complete repository
  with independent callers and change cadence inside an already mixed module.
- Extract only `add_or_update_user`: smaller movement, but it splits one table
  repository across modules and leaves projection ownership duplicated.
- Move workspace preference into `sky/workspaces`: rejected because the
  function is a raw user-row write and must not absorb RBAC policy.
- Move cross-domain cluster joins with the user repository: rejected because
  cluster queries own their transaction, batching, and lifecycle semantics.
- Introduce a generic CRUD repository: rejected because dialect-specific
  upsert and deliberately different projections are the contract, not generic
  CRUD noise.

## Milestones and rollout

1. Add characterization tests against the unchanged facade for signatures,
   decorator depth, row projections, dialect branches, passed-session reuse,
   late-bound dependencies, return shapes, and SQL/engine counts.
2. Move the implementation behind the facade without changing behavior.
3. Run the mapped tests, formatting and static checks, import checks, and
   alternating cold-import and representative SQLite timing probes.
4. Publish only after rebasing onto current `origin/improvements`; merge only
   after all relevant CI and review state is green on the exact branch head.

Rollback is a normal revert.  Public entrypoints and persisted formats remain
unchanged, so no data migration or compatibility transition is required.

## Changed-path-to-test matrix

| Changed path or seam | Tests and checks |
| --- | --- |
| `sky/global_user_state.py` facade, decorators, and patch seams | New user-repository contract; existing global-user-state tests; import contracts |
| SQLite and PostgreSQL upsert construction | New branch and dependency contracts; existing user persistence tests; PostgreSQL CI suite |
| Point, batch, name, partial-name, and all-user projections | New round-trip and field contracts; users, jobs, request, resource-checker, and workspace suites |
| Passed-session reuse and connection budget | New no-extra-engine/session contract; batched cluster and PostgreSQL tests |
| Delete and preferred-workspace writes | New statement and row-count contracts; user server and workspace resolution suites |
| Structural and import behavior | `format.sh --files`, mypy, Pylint, Ruff, BasedPyright, import-linter, compileall, import-order and `git diff --check` |

No live-cloud smoke test is planned because the extraction changes neither
provider calls nor authorization policy.  The relevant smoke behavior is the
same user rows, public calls, projections, transactions, and SQL operations.
