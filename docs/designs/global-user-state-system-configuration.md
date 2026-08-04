# Global User State System Configuration Boundary

_Created: 2026-08-01_

## Problem

`sky/global_user_state.py` is 3,791 lines after four facade-first
decompositions. It still combines cluster lifecycle persistence, storage and
volume state, SSH credentials, cluster YAML, system configuration, and shared
database lifecycle. Size is only a prioritization signal. The useful seam is
the complete three-operation system-configuration repository, not an arbitrary
line-count split.

## Responsibility map

| Responsibility | Callers | Dependencies | State owned | Failure modes | Performance sensitivity | Change cadence |
| --- | --- | --- | --- | --- | --- | --- |
| Shared database lifecycle and schema facade | Server startup, migrations, and every global-state repository | `DatabaseManager`, migration utilities, SQLAlchemy metadata, runtime configuration | Engine, pool, sessions, table identities, migration ordering, historical `_db_manager` replacement seam | Wrong database, pool exhaustion, migration drift, duplicate tables, broken test and embedding isolation | Engine lookup, connection, transaction, and import counts | Database topology and schema evolution |
| Cluster, storage, volume, YAML, and SSH-key persistence | CLI, SDK, dashboard, jobs, Serve, storage, and workspaces | Serialized handles, models, lifecycle statuses, YAML codecs, and provider metadata | Domain rows, lifecycle projections, legacy formats, and shared transactions | State corruption, serialization drift, invalid lifecycle transitions, or extra queries | Query counts, row copies, serialization, and lock ordering | Independent product-domain changes |
| System-configuration repository | API server identity initialization and JWT secret lifecycle | One key-value table, SQLite and PostgreSQL upserts, SQLAlchemy sessions, and wall-clock timestamps | Durable configuration values plus creation and update timestamps | Split server identity, inconsistent first-writer value, overwritten secret, unsupported dialect, lost commit, or extra query | Authentication and startup latency; exact statement and engine-lookup counts | Server topology, authentication, and process-identity policy |

The system-configuration family has materially different callers,
dependencies, durable state, failure behavior, performance sensitivity, and
history from the surrounding repositories. Its three operations own every
read, first-writer initialization, and overwrite transaction for the table.

## Decision

Add `sky/global_user_state_system_config.py` as a plain repository module. Keep
the three historical public functions in `sky.global_user_state` as the stable
facade. They retain their decorators, signatures, module and qualified names,
late-bound `_db_manager` lookup, time sampling, and dialect insert monkeypatch
paths, then delegate once with the resolved engine and dependencies.

The implementation owns system-configuration SQL, dialect selection,
first-writer conflict handling, row projection, and commits. Passing the
existing SQLite and PostgreSQL insert functions preserves the historical
facade patch seam without a reverse import or mutable dependency registry.
This is a facade-first plain-module extraction. No class, protocol, abstract
repository, registry, factory, or dependency-injection layer is introduced.

The extraction is structural only. Keys, values, timestamps, schema, durable
formats, transaction ordering, query shapes, metrics, exceptions, and caller
behavior do not intentionally change.

## Alternatives considered

- Leave the 67-line family in place: safe and low carrying cost, but it retains
  the complete durable configuration gateway inside a facade whose other
  repositories have independent callers and failure policy.
- Move the functions as direct aliases: rejected because their globals would
  stop resolving through the historical `global_user_state._db_manager`,
  dialect insert, and time monkeypatch seams.
- Generalize `config_table` and `system_config_table` behind one key-value
  repository: rejected because their scopes, value codecs, callers, and
  initialization semantics differ.
- Move server identity or JWT-secret policy into the repository: rejected
  because construction and authentication policy belong to their callers, not
  persistence.

## Behavior contract

- Public import paths, signatures, qualified names, decorator depths, and
  exceptions remain unchanged.
- `get_system_config` performs one engine lookup and one read, returning
  `None` for a missing key.
- `get_or_set_system_config` performs one engine lookup, one conflict-safe
  insert, one read, and one commit; concurrent first writers return the one
  durable winner.
- `set_system_config` performs one engine lookup, one dialect-specific upsert,
  and one commit while preserving the original creation timestamp on update.
- SQLite and PostgreSQL insert selection, key/value encoding, timestamps, and
  existing facade monkeypatch paths remain unchanged.

## Milestones and rollout

1. Add and run characterization tests against the unchanged facade for public
   identity, decorator depth, database-manager lookup, statement counts,
   timestamps, first-writer behavior, and dialect dependency lookup.
2. Move only the repository implementation behind the facade.
3. Run the mapped global-state, runtime, token-service, static-analysis,
   import, compile, and performance gates.
4. Publish only from the latest `origin/improvements` and merge only when every
   relevant check and review thread is green on the exact pushed SHA.

Rollback is a normal revert because no schema, data, wire, CLI, or public API
transition is introduced.

## Changed-path-to-test matrix

| Changed path or seam | Tests and checks |
| --- | --- |
| Historical facade, signatures, decorators, `_db_manager`, dialect, and time patch seams | New system-configuration contract; import contracts |
| Read, conflict-safe initialization, overwrite, timestamps, and statement counts | New real-SQLite contract; existing concurrent first-writer test; global-state tests |
| API server identity initialization | Server runtime and server unit tests |
| JWT secret persistence | Token-service tests |
| Structural and type behavior | `format.sh --files`, mypy, Pylint, Ruff, BasedPyright, import-linter, compileall, and `git diff --check` |

No live-cloud smoke is planned because the seam changes no provider, remote
command, process lifecycle, network protocol, or cloud resource behavior.
