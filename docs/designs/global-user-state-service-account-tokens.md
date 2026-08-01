# Global User State Service Account Token Boundary

_Created: 2026-08-01_

## Problem

`sky/global_user_state.py` is 3,897 lines after the schema, operator
notification, and cloud-check repositories were extracted. It still combines
cluster lifecycle persistence, storage and volume state, SSH credentials,
service-account tokens, cluster YAML, and system configuration. Size is only a
prioritization signal. The useful seam is the complete service-account-token
repository, not an arbitrary line-count split.

## Responsibility map

| Responsibility | Callers | Dependencies | State owned | Failure modes | Performance sensitivity | Change cadence |
| --- | --- | --- | --- | --- | --- | --- |
| Shared database lifecycle and schema facade | Server startup, migrations, and every global-state repository | `DatabaseManager`, migration utilities, SQLAlchemy metadata, runtime configuration | Engine, pool, sessions, table identities, migration ordering, historical `_db_manager` replacement seam | Wrong database, pool exhaustion, migration drift, duplicate tables, broken test and embedding isolation | Engine lookup, connection, transaction, and import counts | Database topology and schema evolution |
| Cluster, storage, volume, YAML, SSH-key, and configuration persistence | CLI, SDK, dashboard, jobs, Serve, storage, workspaces | Serialized handles, models, lifecycle statuses, YAML and config codecs | Domain rows, lifecycle projections, legacy formats, shared transactions | State corruption, serialization drift, invalid lifecycle transitions, extra queries | Query counts, row copies, serialization, lock ordering | Independent product-domain changes |
| Service-account-token repository | Request-auth middleware, user administration APIs, managed-job token creation and cleanup, controller teardown | One token table, SQLite and PostgreSQL inserts, SQLAlchemy queries, wall-clock timestamps | Token hash, name, creator, service-account identity, creation/expiry/last-used timestamps | Old JWT accepted after rotation, revoked token accepted, expiry cleanup misses, hash uniqueness failure, timestamp drift, wrong projection, lost commit | Auth lookup latency, exactly one engine lookup and one SQL statement per ordinary operation | Authentication policy, token lifecycle, managed-job cleanup |

The token family has materially different callers, dependencies, durable state,
failure behavior, performance sensitivity, and history from the surrounding
repositories. All nine operations own one complete session and transaction or
read boundary over the same table.

## Decision

Add `sky/global_user_state_service_account_tokens.py` as a plain repository
module. Keep the nine historical public functions in `sky.global_user_state`
as the stable facade. They retain their decorators, signatures, module and
qualified names, time sampling, dialect dependency lookup, and late-bound
`_db_manager` access, then delegate once with the resolved engine and existing
SQLAlchemy dependencies.

The implementation owns token SQL, row projection, LIKE escaping, commits, and
the not-found rotation decision. Passing the existing session factory and
dialect insert functions keeps historical facade monkeypatch paths working
without a reverse import or mutable dependency registry. This is a facade-first
plain-module extraction. No class, protocol, abstract repository, registry,
factory, or dependency-injection framework is introduced.

The extraction is structural only. Token formats and hashing remain owned by
`sky.users.token_service`; user and managed-job lifecycle ordering remains at
the callers. Database schema, persisted values, query shapes, transaction
boundaries, retries, metrics decorators, and API responses do not intentionally
change.

## Alternatives considered

- Move public functions as direct aliases: rejected because their globals would
  stop resolving through the historical `global_user_state._db_manager`,
  `orm.Session`, dialect insert, and time monkeypatch seams.
- Import the facade from the implementation: rejected because it creates a
  reverse dependency and circular-import risk.
- Move token creation, JWT encoding, permissions, and API presentation too:
  rejected because those are separate service and transport responsibilities.
- Create a generic table repository: rejected because it erases token-specific
  rotation, expiry, hash lookup, and projection semantics.
- Leave the code in place: safe, but retains a complete authentication
  repository whose reasons to change are independent of cluster, storage,
  volume, YAML, and system configuration persistence.

## Behavior contract

- Public import paths, signatures, qualified names, decorator depths, and
  exceptions remain unchanged.
- Token IDs, names, SHA-256 hashes, timestamps, creator and service-account
  identities, row projections, and LIKE escaping remain unchanged.
- Rotation replaces the hash and expiry, clears last use, resets creation time,
  commits before raising on a missing token, and invalidates the old JWT lookup.
- Every public call performs one facade database-manager lookup. Ordinary
  operations issue one SQL statement and writes commit once.
- SQLite and PostgreSQL insert selection remains unchanged.
- Existing patches of `sky.global_user_state` functions and private SQL/time
  dependencies remain effective.

## Milestones and rollout

1. Add and run characterization tests against the unchanged facade for public
   identity, decorator depth, database-manager lookup, SQL counts, projection,
   rotation, expiry filtering, and old private monkeypatch paths.
2. Move only the repository implementation behind the facade.
3. Run the mapped unit, auth middleware, user API, jobs, controller, global
   state, static-analysis, import, compile, and performance gates.
4. Publish only from the latest `origin/improvements` and merge only when every
   relevant check and review thread is green on the exact pushed SHA.

Rollback is a normal revert because no schema, data, wire, CLI, or public API
transition is introduced.

## Changed-path-to-test matrix

| Changed path or seam | Tests and checks |
| --- | --- |
| Historical facade, signatures, decorators, `_db_manager`, `orm`, dialect, and time patch seams | New token repository contract; existing global-state token tests; import contracts |
| CRUD, hash lookup, rotation, last-used, expiry, and row projection | Existing global-state token tests; new real-DB round trip and statement counts |
| Request authentication | Bearer-token middleware and common-auth tests |
| User token administration and permissions | Service-account token, permission, deletion-protection, and user server tests |
| Managed-job token lifecycle | Jobs utils, jobs server core, and jobs controller tests |
| Structural and type behavior | `format.sh --files`, mypy, Pylint, Ruff, BasedPyright, import-linter, compileall, and `git diff --check` |

No live-cloud smoke is planned because the seam changes no provider, remote
command, process lifecycle, network protocol, or cloud resource behavior.
