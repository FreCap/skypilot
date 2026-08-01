# Global user state cloud-check cache extraction

## Context

`sky/global_user_state.py` is the stable persistence facade for clusters,
users, storage, volumes, configuration, credentials, service-account tokens,
and operator notifications.  After the schema and operator-notification
repositories moved behind that facade, the file is still 4,013 lines.

Line count is only a prioritization signal.  This extraction is justified by a
complete repository seam: the enabled-cloud, allowed-cloud, and detailed check
result rows share one key namespace and one caller family, while depending on
none of the cluster, storage, volume, migration, or notification lifecycle.

## Before: responsibility map

| Responsibility | Callers | Dependencies | State owned | Failure modes | Performance sensitivity | Change cadence |
| --- | --- | --- | --- | --- | --- | --- |
| Shared database lifecycle and schema facade | Server startup, every global-state repository, migrations, tests | `DatabaseManager`, SQLAlchemy metadata, migration utilities | Engine, pool, sessions, table identities, migration ordering | Wrong database, duplicate tables, migration drift, broken monkeypatch seams | Cold initialization, connection and transaction counts | Database topology and schema evolution |
| Cluster, user, credential, storage, volume, and configuration persistence | CLI, SDK, dashboard, jobs, Serve, auth, workspaces | Domain models, serialized handles, lifecycle statuses, YAML and config codecs | Durable domain rows and lifecycle projections | State corruption, serialization drift, invalid transitions, extra queries | Query counts, row copies, serialization | Independent product-domain changes |
| Cloud-check cache persistence and projection | `sky.check`, `sky.core`, check-result tests | Config table, JSON, cloud registry, SQLite/PostgreSQL upserts | `enabled_clouds_*`, `allowed_clouds_*`, and `check_results_*` rows | Key drift, unsupported dialect, corrupt JSON, scoped merge clobbering, removed-cloud lookup failure | One engine lookup and one SQL statement for simple reads/writes; scoped check updates add one read | Credential-check policy, workspace filtering, provider registry, dashboard check-result presentation |
| Operator-notification persistence | Operator notification APIs and workers | Notification tables, dialect-specific sequence allocation | Notification events, cursors, and sequences | Duplicate sequence, cursor drift, transaction rollback | Transaction and sequence allocation counts | Operator event delivery |

The cloud-check family has materially different callers, dependencies, durable
keys, failure behavior, and change history from the surrounding domain
repositories.  All six public operations can delegate after obtaining the
current facade-owned engine, and each implementation can own its complete
session and transaction boundary.

## Decision

Add `sky/global_user_state_cloud_checks.py` as a plain repository module.

The historical `sky.global_user_state` functions remain the public facade and
retain:

- their import paths, signatures, module identities, and timing decorators;
- late-bound lookup of `sky.global_user_state._db_manager`, which tests and
  embedders patch;
- the `sky.global_user_state` logger identity for corrupt-row warnings.

Each facade function obtains the engine exactly once and delegates to one
implementation function.  The implementation owns the SQLAlchemy session,
dialect selection, key construction, JSON projection, commit, and warning
decision.  Key helpers remain available at their historical protected paths as
direct aliases, because they are stateless and require no facade-owned state.

This is facade-first decomposition using functions and a module.  A class,
protocol, abstract repository, registry, strategy, or dependency-injection
layer would invent a second implementation that does not exist.  Directly
moving the public functions is not viable because importing the facade-owned
database manager back from the implementation would create a circular import
and would weaken the historical monkeypatch seam.

## Behavior contract

- Database keys and JSON formats do not change.
- SQLite and PostgreSQL `INSERT ... ON CONFLICT DO UPDATE` statements do not
  change.
- Full check-result writes replace the row.  Scoped writes retain their current
  context-level read-modify-write behavior and documented race envelope.
- Missing rows return the same empty values, corrupt check-result rows log from
  `sky.global_user_state`, and removed cloud implementations remain ignored.
- Every public call performs one facade database-manager lookup.  Simple reads
  and writes issue one SQL statement; scoped check-result updates issue one
  read and one upsert.
- No schema, migration, CLI, wire, configuration, serialization, or lifecycle
  contract changes.

## Alternatives

- Leave the functions in the facade: safe, but retains a complete repository
  whose callers and reasons to change are independent from the surrounding
  domain persistence.
- Move all config-table access: too broad.  System configuration and other
  configuration rows have different callers and behavior.
- Create a generic key-value repository: rejected because it would erase the
  cloud-check merge and projection semantics and add a parallel abstraction.
- Consolidate the repeated upsert code: deferred.  This change is structural;
  query construction stays byte-for-byte equivalent where practical.

## Milestones and rollout

1. Add characterization tests against the unchanged facade for signatures,
   key formats, late-bound database-manager lookup, row projections, warning
   identity, and SQL statement counts.
2. Move the implementation behind the facade without changing behavior.
3. Run the mapped tests, formatting and static checks, import checks, and
   alternating cold-import and operation timing probes.
4. Publish only after the exact branch head is based on current
   `origin/improvements`; merge only after all relevant CI and review state is
   green on that SHA.

Rollback is a normal revert.  The public facade and persisted formats remain
unchanged, so no data migration or compatibility transition is required.

## Changed-path-to-test matrix

| Changed path or seam | Tests and checks |
| --- | --- |
| `sky/global_user_state.py` public facade and `_db_manager` patch seam | New cloud-check contract test; global-user-state tests; import contracts |
| Enabled and allowed cloud cache rows | New contract round trip and key assertions; `tests/test_global_user_state.py`; `tests/unit_tests/test_sky/test_check.py` |
| Detailed check-result replacement and scoped merge | New contract; `test_global_user_state_check_results.py`; `test_check_persistence.py` |
| SQLite/PostgreSQL statement construction | New SQLite statement-count contract; compile both dialect statements in the implementation contract; existing PostgreSQL CI suite |
| `sky.check` and `sky.core` callers | Check unit and persistence suites; core tests |
| Structural and import behavior | `format.sh --files`, Ruff, mypy, Pylint, import-linter, compileall, `git diff --check` |

No live-cloud smoke is planned because the seam changes neither provider calls
nor credential checking.  The relevant smoke behavior is represented by the
same database keys, public calls, result projections, and SQL operations.
